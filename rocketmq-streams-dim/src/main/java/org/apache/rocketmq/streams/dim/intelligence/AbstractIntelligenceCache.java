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
package org.apache.rocketmq.streams.dim.intelligence;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.CommonRequest;
import com.aliyuncs.CommonResponse;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.profile.DefaultProfile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.dboperator.IDBDriver;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIntelligenceCache extends BasedConfigurable implements
    IAfterConfigurableRefreshListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractIntelligenceCache.class);

    public static final String TYPE = "intelligence";

    protected static final int FILE_MAX_LINE = 50000;//每个文件最大行数

    protected transient IntValueKV intValueKV = new IntValueKV(0) {
        @Override
        public Integer get(String key) {
            return null;
        }

        @Override
        public void put(String key, Integer value) {

        }
    };

    protected String idFieldName = "id";//必须有id字段

    protected int batchSize = 3000;

    @ENVDependence
    protected Long pollingTimeMinute = 30L;

    protected String datasourceName;//情报对应的存储

    protected transient IDBDriver outputDataSource;

    protected static ExecutorService executorService;

    protected transient ScheduledExecutorService scheduledExecutorService;

    public AbstractIntelligenceCache() {
        setType(TYPE);
        executorService = ThreadPoolFactory.createFixedThreadPool(20, AbstractIntelligenceCache.class.getName() + "-intelligence");
        scheduledExecutorService = new ScheduledThreadPoolExecutor(3);
    }

    public IntValueKV startLoadData(String sql, IDBDriver resource) {
        try {
            String statisticalSQL = sql;
            int startIndex = sql.toLowerCase().indexOf("from");
            statisticalSQL = "select count(1) as c, min(" + idFieldName + ") as min, max(" + idFieldName + ") as max " + sql.substring(startIndex);
            List<Map<String, Object>> rows = resource.queryForList(statisticalSQL);
            Map<String, Object> row = rows.get(0);
            int count = Integer.parseInt(row.get("c").toString());
            IntValueKV intValueKV = new IntValueKV(count);
            //int maxBatch=count/maxSyncCount;//每1w条数据，一个并发。如果数据量比较大，为了提高性能，并行执行
            if (count == 0) {
                return new IntValueKV(0) {
                    @Override
                    public Integer get(String key) {
                        return null;
                    }

                    @Override
                    public void put(String key, Integer value) {

                    }
                };
            }
            long min = Long.parseLong(row.get("min").toString());
            long max = Long.parseLong(row.get("max").toString());
            int maxSyncCount = count / FILE_MAX_LINE + 1;
            long step = (max - min + 1) / maxSyncCount;
            CountDownLatch countDownLatch = new CountDownLatch(maxSyncCount + 1);
            AtomicInteger finishedCount = new AtomicInteger(0);
            String taskSQL = null;
            if (sql.contains(" where ")) {
                taskSQL = sql + " and " + idFieldName + ">#{startIndex} and " + idFieldName + "<=#{endIndex} order by " + idFieldName + " limit " + batchSize;
            } else {
                taskSQL = sql + " where " + idFieldName + ">#{startIndex} and " + idFieldName + "<=#{endIndex} order by " + idFieldName + " limit " + batchSize;
            }

            int i = 0;
            for (; i < maxSyncCount; i++) {
                FetchDataTask fetchDataTask = new FetchDataTask(taskSQL, (min - 1) + step * i, (min - 1) + step * (i + 1), countDownLatch, finishedCount, resource, i, intValueKV, this, count);
                executorService.execute(fetchDataTask);
            }
            FetchDataTask fetchDataTask = new FetchDataTask(taskSQL, (min - 1) + step * i, (min - 1) + step * (i + 1), countDownLatch, finishedCount, resource, i, intValueKV, this, count);
            executorService.execute(fetchDataTask);
            countDownLatch.await();
            LOGGER.info("[{}] {} load data finish, load data line  size is {}", getConfigureName(), getClass().getSimpleName(), intValueKV.getSize());
            return intValueKV;
        } catch (Exception e) {
            LOGGER.error("[{}] failed loading intelligence data!", getConfigureName(), e);
            return new IntValueKV(0) {
                @Override
                public Integer get(String key) {
                    return null;
                }

                @Override
                public void put(String key, Integer value) {

                }
            };
        }
    }

    protected transient AtomicBoolean hasInit = new AtomicBoolean(false);

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        this.outputDataSource = configurableService.queryConfigurable(ISink.TYPE, datasourceName);
    }

    public void startIntelligence() {
        boolean success = dbInit();
        if (success) {
            startIntelligenceInner();
        } else {
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    boolean success = false;
                    while (!success) {
                        success = dbInit();
                        try {
                            Thread.sleep(60 * 1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    startIntelligenceInner();
                }
            });
            thread.start();
        }
    }

    public void startIntelligenceInner() {
        String sql = getSQL();
        if (hasInit.compareAndSet(false, true)) {
            this.intValueKV = startLoadData(sql, outputDataSource);
            scheduledExecutorService.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    intValueKV = startLoadData(sql, outputDataSource);
                }
            }, pollingTimeMinute, pollingTimeMinute, TimeUnit.MINUTES);
        }
    }

    public abstract Map<String, Object> getRow(String key);

    /**
     * 查询情报需要的sql
     */
    protected abstract String getSQL();

    /**
     * 情报中的 情报字段名
     */
    public abstract String getKeyName();

    /**
     * 情报对应的表名
     */
    public abstract String getTableName();

    protected class FetchDataTask implements Runnable {
        IntValueKV intValueKV;
        long startIndex;
        long endIndex;
        String sql;
        CountDownLatch countDownLatch;
        int index;
        IDBDriver resource;
        AtomicInteger finishedCount;//完成了多少条
        AbstractIntelligenceCache cache;
        int totalSize;//一共有多少条数据

        public FetchDataTask(String sql, long startIndex, long endIndex, CountDownLatch countDownLatch,
            AtomicInteger finishedCount, IDBDriver resource, int i, IntValueKV intValueKV,
            AbstractIntelligenceCache cache, int totalSize) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.countDownLatch = countDownLatch;
            this.sql = sql;
            this.finishedCount = finishedCount;
            this.resource = resource;
            this.index = i;
            this.intValueKV = intValueKV;
            this.cache = cache;
            this.totalSize = totalSize;
        }

        @Override
        public void run() {
            long currentIndex = startIndex;
            JSONObject msg = new JSONObject();
            msg.put("endIndex", endIndex);
            while (true) {
                try {
                    msg.put("startIndex", currentIndex);
                    String sql = SQLUtil.parseIbatisSQL(msg, this.sql);
                    List<Map<String, Object>> rows = resource.queryForList(sql);
                    if (rows == null || rows.size() == 0) {
                        break;
                    }
                    currentIndex = Long.parseLong(rows.get(rows.size() - 1).get(idFieldName).toString());

                    int size = rows.size();
                    int count = finishedCount.addAndGet(size);
                    double progress = (double) count / (double) totalSize;
                    progress = progress * 100;
                    LOGGER.info("[{}] {}, finished count is {} the total count is {}, the progress is {}%", getConfigureName(), cache.getClass().getSimpleName(), count, totalSize, String.format("%.2f", progress));
                    if (size < batchSize) {
                        if (size > 0) {
                            doProccRows(intValueKV, rows, index);
                        }
                        break;
                    }
                    doProccRows(intValueKV, rows, index);
                } catch (Exception e) {
                    throw new RuntimeException("put data error ", e);
                }
            }

            countDownLatch.countDown();
        }
    }

    public boolean dbInit() {
        try {
            int successCode = 200;
            String region = ComponentCreator.getProperties().getProperty(ConfigureFileKey.INTELLIGENCE_REGION);
            String ak = ComponentCreator.getProperties().getProperty(ConfigureFileKey.INTELLIGENCE_AK);
            String sk = ComponentCreator.getProperties().getProperty(ConfigureFileKey.INTELLIGENCE_SK);
            String endpoint = ComponentCreator.getProperties().getProperty(ConfigureFileKey.INTELLIGENCE_TIP_DB_ENDPOINT);
            if (StringUtils.isNotBlank(region) && StringUtils.isNotBlank(ak) && StringUtils.isNotBlank(sk) && StringUtils.isNotBlank(endpoint)) {
                DefaultProfile profile = DefaultProfile.getProfile(region, ak, sk);
                IAcsClient client = new DefaultAcsClient(profile);
                CommonRequest request = new CommonRequest();
                request.setDomain(endpoint);
                request.setVersion("2016-03-16");
                request.setAction("DescribeDataSource");
                CommonResponse response = client.getCommonResponse(request);
                int code = response.getHttpStatus();
                if (successCode == code) {
                    String content = response.getData();
                    if (StringUtils.isNotBlank(content)) {
                        JSONObject obj = JSON.parseObject(content);
                        JSONObject dbInfo = obj.getJSONObject("dBInfo");
                        if (dbInfo != null) {
                            String dbUrl = "jdbc:mysql://" + dbInfo.getString("dbConnection") + ":" + dbInfo.getInteger("port") + "/" + dbInfo.getString("dBName");
                            String dbUserName = dbInfo.getString("userName");
                            String dbPassword = dbInfo.getString("passWord");
                            JDBCDriver dataSource = (JDBCDriver) this.outputDataSource;
                            dataSource.setUrl(dbUrl);
                            dataSource.setPassword(dbPassword);
                            dataSource.setUserName(dbUserName);
                            dataSource.setHasInit(false);
                            dataSource.init();
                            LOGGER.debug("[{}] succeed in getting db information from tip service!", getConfigureName());
                            return true;
                        }
                    }
                }
            }
            LOGGER.error("[{}] failed in getting db information from tip service!", getConfigureName());
            return false;
        } catch (Exception e) {
            LOGGER.error("[{}] failed in getting db information from tip service!", getConfigureName(), e);
            return false;
        }
    }

    /**
     * 把存储0/1字符串的值，转化成bit
     */
    protected int createInt(List<String> values) {
        return NumberUtils.createBitMapInt(values);
    }

    /**
     * 获取某位的值，如果是1，返回字符串1，否则返回null
     */
    protected String getNumBitValue(int num, int i) {
        boolean exist = NumberUtils.getNumFromBitMapInt(num, i);
        if (exist) {
            return "1";
        }
        return null;
    }

    protected abstract void doProccRows(IntValueKV intValueKV, List<Map<String, Object>> rows, int index);

    public String getIdFieldName() {
        return idFieldName;
    }

    public void setIdFieldName(String idFieldName) {
        this.idFieldName = idFieldName;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Long getPollingTimeMinute() {
        return pollingTimeMinute;
    }

    public void setPollingTimeMinute(Long pollingTimeMinute) {
        this.pollingTimeMinute = pollingTimeMinute;
    }

    public String getDatasourceName() {
        return datasourceName;
    }

    public void setDatasourceName(String datasourceName) {
        this.datasourceName = datasourceName;
    }
}
