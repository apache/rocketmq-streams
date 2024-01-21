/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.huawei.obs;

import com.google.common.collect.Lists;
import com.obs.services.ObsClient;

import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractFlinkSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.huawei.obs.iterator.ObsDataIterator;
import org.apache.rocketmq.streams.huawei.obs.split.ObsSplit;
import org.apache.rocketmq.streams.lease.service.ILeaseStorage;
import org.apache.rocketmq.streams.lease.service.impl.LeaseServiceImpl;
import org.apache.rocketmq.streams.lease.service.storages.DBLeaseStorage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObsSource extends AbstractPullSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObsSource.class);

    private String accessId;

    private String accessKey;

    private String endPoint;

    private String bucketName;

    private String filePath;

    private String fileType;

    private String compressType;

    private String startTime;

    private String endTime;

    private Integer cycle;

    private String cycleUnit;

    private Integer cycleT = 1;

    /****************************其上为成员变量*****************************************/
    private ObsClient obsClient;

    private LeaseServiceImpl leaseService;

    private long cycleTime = 0L;

    private ScheduledExecutorService scheduledExecutorService;

    @Override
    protected boolean initConfigurable() {
        this.isTest = true;
        super.initConfigurable();
        if (this.cache.getKeyConfig(this.getNameSpace() + "_" + this.getName(), START_TIME) != null) {
            this.startTime = this.cache.getKeyConfig(this.getNameSpace() + "_" + this.getName(), START_TIME);
        } else if (this.startTime == null) {
            this.startTime = DateUtil.getCurrentTimeString("yyyy-MM-dd HH:mm:ss");
        }

        if (this.obsClient == null) {
            this.obsClient = new ObsClient(accessId, accessKey, endPoint);
        }

        if (cycleUnit == null || cycleUnit.isEmpty()) {
            cycleUnit = TimeUnit.SECONDS.toString();
        }
        if (cycleUnit.equalsIgnoreCase(TimeUnit.DAYS.toString())) {
            cycleTime = cycle * 24 * 60 * 60 * 1000;
        } else if (cycleUnit.equalsIgnoreCase(TimeUnit.HOURS.toString())) {
            cycleTime = cycle * 60 * 60 * 1000;
        } else if (cycleUnit.equalsIgnoreCase(TimeUnit.MINUTES.toString())) {
            cycleTime = cycle * 60 * 1000;
        } else if (cycleUnit.equalsIgnoreCase(TimeUnit.SECONDS.toString())) {
            cycleTime = cycle * 1000;
        }

        if (fileType == null || fileType.isEmpty()) {
            fileType = "json";
        }

        if (compressType == null || compressType.isEmpty()) {
            compressType = "gzip";
        }

        String jdbcDriver = getConfiguration().getProperty(ConfigurationKey.JDBC_DRIVER, ConfigurationKey.DEFAULT_JDBC_DRIVER);
        String url = getConfiguration().getProperty(ConfigurationKey.JDBC_URL);
        String userName = getConfiguration().getProperty(ConfigurationKey.JDBC_USERNAME);
        String password = getConfiguration().getProperty(ConfigurationKey.JDBC_PASSWORD);

        this.leaseService = new LeaseServiceImpl();
        ILeaseStorage storage = new DBLeaseStorage(jdbcDriver, url, userName, password);
        this.leaseService.setLeaseStorage(storage);

        //启动Master选举
        this.leaseService.startLeaseTask("Batch_Job_Lock_" + this.getNameSpace() + "_" + this.getName(), 60, nextLeaseDate -> {
            LOGGER.info("[{}][{}][{}] Batch_Job_Locked_Success", this.getNameSpace(), this.getName(), IdUtil.objectId(this));
        });

        if (this.scheduledExecutorService == null) {
            this.scheduledExecutorService = new ScheduledThreadPoolExecutor(10, new BasicThreadFactory.Builder().namingPattern(getNameSpace() + "-" + getName() + "-schedule-%d").build());
        }
        this.scheduledExecutorService.scheduleWithFixedDelay(() -> {
            if (this.leaseService.hasLease("Batch_Job_Lock_" + this.getNameSpace() + "_" + this.getName())) {
                LOGGER.info("[{}][{}][{}] Batch_Job_Master", this.getNameSpace(), this.getName(), IdUtil.objectId(this));

                long start = DateUtil.parse(this.startTime).getTime();
                long end = (this.endTime == null ? System.currentTimeMillis() : DateUtil.parse(this.endTime).getTime()) - cycleT * cycleTime;
                if (start > end) {
                    LOGGER.warn("[{}][{}][{}] Batch_Jobs_Start({})_Greater_Then_End({})", this.getNameSpace(), this.getName(), IdUtil.objectId(this), DateFormatUtils.format(start, "yyyyMMddHHmmss"), DateFormatUtils.format(end, "yyyyMMddHHmmss"));
                    this.cache.putKeyConfig(this.getNameSpace() + "_" + this.getName(), START_TIME, this.startTime);
                } else {
                    List<Date> dateList = DateUtil.getWindowBeginTime(end, cycleTime, end - start);

                    LOGGER.info("[{}][{}][{}] Batch_Jobs_From({})_To({})_Size({})", this.getNameSpace(), this.getName(), IdUtil.objectId(this), DateFormatUtils.format(start, "yyyyMMddHHmmss"), DateFormatUtils.format(end, "yyyyMMddHHmmss"), dateList.size());
                    if (!dateList.isEmpty()) {
                        for (Date date : dateList) {
                            String partition = partitionPath(date, filePath);
                            this.cache.putKeyConfig(this.getNameSpace() + "_" + this.getName() + "_partition", partition + "___" + DateUtil.format(date), DateUtil.format(new Date(start)));
                        }
                        this.startTime = DateUtil.format(new Date(end));
                        this.cache.putKeyConfig(this.getNameSpace() + "_" + this.getName(), START_TIME, this.startTime);
                    }
                }
            }
        }, 0, 60, TimeUnit.SECONDS);

        return true;
    }

    private String partitionPath(Date partitionTime, String filePath) {
        String year = DateUtil.getYear(partitionTime);
        String month = DateUtil.getMonth(partitionTime) < 10 ? "0" + DateUtil.getMonth(partitionTime) : String.valueOf(DateUtil.getMonth(partitionTime));
        String day = DateUtil.getDay(partitionTime) < 10 ? "0" + DateUtil.getDay(partitionTime) : String.valueOf(DateUtil.getDay(partitionTime));
        String hour = DateUtil.getHour(partitionTime) < 10 ? "0" + DateUtil.getHour(partitionTime) : String.valueOf(DateUtil.getHour(partitionTime));
        String minute = DateUtil.getMinute(partitionTime) < 10 ? "0" + DateUtil.getMinute(partitionTime) : String.valueOf(DateUtil.getMinute(partitionTime));
        return filePath.replaceAll("%Y", year).replaceAll("%m", month).replaceAll("%d", day).replaceAll("%H", hour).replaceAll("%M", minute);
    }

    @Override
    public String loadSplitOffset(ISplit<?, ?> split) {
        return this.startTime;
    }

    @Override
    protected ISplitReader createSplitReader(ISplit<?, ?> split) {

        return new AbstractFlinkSplitReader(split) {

            private String queueId;
            private String partition;
            private Boolean hasNext = true;

            @Override
            public String getCursor() {
                return null;
            }

            @Override
            public void open() {
                ObsSplit obsSplit = (ObsSplit)split;
                this.queueId = obsSplit.getQueueId();
                this.partition = this.queueId.substring(0, this.queueId.indexOf("___"));
            }

            @Override
            public boolean next() {
                return this.hasNext;
            }

            @Override
            public Iterator<PullMessage<?>> getMessage() {
                return new ObsDataIterator(obsClient, bucketName, this.partition, fileType, compressType, fieldDelimiter, getMetaData(), () -> {
                    doSplitRelease(Lists.newArrayList(split));
                    cache.deleteKeyConfig(getNameSpace() + "_" + getName() + "_partition", this.queueId);
                    this.hasNext = false;
                });
            }

            @Override
            public void seek(String cursor) {
            }

            @Override
            public long getDelay() {
                return 0;
            }

            @Override
            public long getFetchedDelay() {
                return 0;
            }

        };
    }

    @Override
    public void destroySource() {
        if (this.obsClient != null) {
            try {
                this.obsClient.close();
                ScheduleFactory.getInstance().cancel("ObsSource_Job_" + this.getNameSpace() + "_" + this.getName());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public List<ISplit<?, ?>> fetchAllSplits() {
        Map<String, String> partitions = this.cache.getKVListByNameSpace(this.getNameSpace() + "_" + this.getName() + "_partition");
        List<ISplit<?, ?>> splits = Lists.newArrayList();
        if (partitions != null && !partitions.isEmpty()) {
            for (Map.Entry<String, String> entry : partitions.entrySet()) {
                ObsSplit split = new ObsSplit(entry.getKey());
                splits.add(split);
            }
        }
        return splits;
    }

    public String getAccessId() {
        return accessId;
    }

    public void setAccessId(String accessId) {
        this.accessId = accessId;
    }

    public String getAccessKey() {
        return accessKey;
    }

    public void setAccessKey(String accessKey) {
        this.accessKey = accessKey;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public Integer getCycle() {
        return cycle;
    }

    public void setCycle(Integer cycle) {
        this.cycle = cycle;
    }

    public String getCycleUnit() {
        return cycleUnit;
    }

    public void setCycleUnit(String cycleUnit) {
        this.cycleUnit = cycleUnit;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileType() {
        return fileType;
    }

    public void setFileType(String fileType) {
        this.fileType = fileType;
    }

    public String getCompressType() {
        return compressType;
    }

    public void setCompressType(String compressType) {
        this.compressType = compressType;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public long getCycleTime() {
        return cycleTime;
    }

    public void setCycleTime(long cycleTime) {
        this.cycleTime = cycleTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public Integer getCycleT() {
        return cycleT;
    }

    public void setCycleT(Integer cycleT) {
        this.cycleT = cycleT;
    }
}
