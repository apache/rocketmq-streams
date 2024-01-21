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
package org.apache.rocketmq.streams.tencent.waf;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.RFC4180Parser;
import com.opencsv.exceptions.CsvValidationException;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.tencent.TencentOpenAPIClient;
import org.apache.rocketmq.streams.tencent.TencentOpenAPISplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TencentWafAttackSource extends AbstractPullSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TencentWafAttackSource.class);
    protected final int BUFFER_SIZE = 10240;
    protected String endPoint;
    protected String accessId;
    protected String accessKey;
    protected String version;
    protected String region;
    protected String domain;
    protected String queryString;
    protected String sort;
    protected String taskNameField;
    protected String startField;
    protected String endField;
    /**
     * 每10分钟拉去一次
     */
    protected Long pollingMinute = 10L;
    /**
     * 在一个周期内的并发性
     */
    protected int splitCount = 1;
    /**
     * 调度开始时间
     */
    protected String initFirstTime = DateUtil.getCurrentTimeString();

    protected int retryTimes = 10;

    protected int sleepTime = 10000;

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        return new AbstractSplitReader(split) {
            private String id;
            private long startTime;
            private long dateAdd;
            private long pollingMinute;
            private int pageNum = 1;

            private transient TencentOpenAPIClient tencentOpenAPIClient;

            @Override
            public void open() {
                Credential cred = new Credential(accessId, accessKey);
                TencentOpenAPISplit openAPISplit = (TencentOpenAPISplit)split;
                this.id = openAPISplit.getQueueId();
                this.startTime = openAPISplit.getInitStartTime();
                this.dateAdd = openAPISplit.getDateAdd();
                this.pollingMinute = openAPISplit.getPollingMinute();
                this.tencentOpenAPIClient = new TencentOpenAPIClient(endPoint, version, cred, region);
            }

            @Override
            public boolean next() {
                return this.startTime + this.pollingMinute * 60 * 1000 < System.currentTimeMillis();
            }

            @Override
            public Iterator<PullMessage<?>> getMessage() {
                Iterator<PullMessage<?>> iterator = null;
                Map<String, Object> parameters = new HashMap<>();
                if (StringUtil.isNotEmpty(startField)) {
                    parameters.put(startField, createTime(startTime));
                    //parameters.put(startField, "2023-03-27T00:45:09+08:00");
                }

                long endTime = startTime + dateAdd * 60 * 1000;
                if (StringUtil.isNotEmpty(endField)) {
                    parameters.put(endField, createTime(endTime));
                }
                if (StringUtil.isNotEmpty(domain)) {
                    parameters.put("Domain", domain);
                }
                if (StringUtil.isNotEmpty(queryString)) {
                    parameters.put("QueryString", queryString);
                }
                if (StringUtil.isNotEmpty(sort)) {
                    parameters.put("Sort", sort);
                } else {
                    parameters.put("Sort", "desc");
                }

                String taskName = this.id + "_" + DateUtil.format(new Date(startTime), "yyyyMMddHHmmss") + "_" + DateUtil.format(new Date(endTime), "yyyyMMddHHmmss");

                if (StringUtil.isNotEmpty(taskNameField)) {
                    parameters.put(taskNameField, taskName);
                }
                try {
                    String payLoad = JSONObject.toJSONString(parameters);
                    String resultString = tencentOpenAPIClient.call("PostAttackDownloadTask", payLoad);
                    LOGGER.info("[{}][{}] Execute_Openapi_Query({})_Result({})_Query_Time_Gap({}-{})_Query_Page({})", IdUtil.instanceId(), getName(), payLoad, resultString, DateUtil.format(new Date(startTime)), DateUtil.format(new Date(endTime)), pageNum);
                    JSONObject result = JSONObject.parseObject(resultString);
                    if (result != null) {
                        JSONObject data = result.getJSONObject("Response");
                        if (data != null) {
                            String taskId = data.getString("Flow");
                            if (taskId != null) {
                                String url = null;
                                int times = 1;
                                do {
                                    String rspStr = tencentOpenAPIClient.call("GetAttackDownloadRecords", "{}");
                                    JSONObject rsp = JSONObject.parseObject(rspStr).getJSONObject("Response");
                                    JSONArray array = rsp.getJSONArray("Records");
                                    LOGGER.info("[{}][{}] Execute_Openapi_Download_ResultSize({})", IdUtil.instanceId(), getName(), array.size());
                                    for (int i = 0; i < array.size(); i++) {
                                        JSONObject jsonObject = array.getJSONObject(i);
                                        if (jsonObject.getString("Id") != null && taskId.equals(jsonObject.getString("Id"))) {
                                            url = jsonObject.getString("Url");
                                            if (url == null || url.isEmpty()) {
                                                Thread.sleep(sleepTime);
                                            }
                                            break;
                                        }
                                    }
                                    times++;
                                }
                                while (times <= retryTimes && (url == null || url.isEmpty()));
                                if (url != null && !url.isEmpty()) {
                                    try {
                                        iterator = extract(download(url)).iterator();
                                    } catch (Exception e) {
                                        throw new RuntimeException(e);
                                    }
                                } else {
                                    LOGGER.info("[{}][{}] Can_Not_Get_The_Url_After_Retry_Times({})", IdUtil.instanceId(), getName(), retryTimes);
                                }

                            }
                        }
                    }
                    this.startTime = this.startTime + this.pollingMinute * 60 * 1000;
                    this.pageNum = 1;
                } catch (TencentCloudSDKException | InterruptedException e) {
                    LOGGER.error("Tencent_OpenAPI_Call_Error", e);
                }
                return iterator;
            }

            @Override
            public long getDelay() {
                return System.currentTimeMillis() - startTime;
            }

            @Override
            public long getFetchedDelay() {
                return 0;
            }

            @Override
            public void seek(String cursor) {
                if (cursor == null) {
                    this.cursor = this.startTime + ";" + (pageNum + 10000);
                    return;
                }
                this.cursor = cursor;
                String[] values = cursor.split(";");
                this.startTime = Long.parseLong(values[0]);
                this.pageNum = Integer.parseInt(values[1]) - 10000;
            }

            @Override
            public String getCursor() {
                this.cursor = this.startTime + ";" + (pageNum + 10000);
                return this.cursor;
            }

            private byte[] download(String url) throws Exception {
                byte[] tarData;
                try (InputStream inputStream = new URL(url).openStream()) {
                    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                    byte[] buffer = new byte[BUFFER_SIZE];
                    int n;
                    while ((n = inputStream.read(buffer, 0, BUFFER_SIZE)) >= 0) {
                        outputStream.write(buffer, 0, n);
                    }
                    outputStream.close();
                    tarData = outputStream.toByteArray();
                }
                return tarData;
            }

            public List<PullMessage<?>> extract(byte[] tarData) throws IOException, CsvValidationException {
                List<PullMessage<?>> msgs = new ArrayList<>();
                try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(tarData); GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream); TarArchiveInputStream tarArchiveInputStream = new TarArchiveInputStream(gzipInputStream)) {
                    TarArchiveEntry tarEntry;
                    while ((tarEntry = tarArchiveInputStream.getNextTarEntry()) != null) {
                        if (tarEntry.isDirectory()) {
                            continue;
                        }
                        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                        byte[] buffer = new byte[BUFFER_SIZE];
                        int n;
                        while ((n = tarArchiveInputStream.read(buffer, 0, BUFFER_SIZE)) >= 0) {
                            outputStream.write(buffer, 0, n);
                        }
                        outputStream.close();
                        byte[] fileContent = outputStream.toByteArray();
                        // 将CSV数据解析为Java对象
                        ByteArrayInputStream fileInputStream = new ByteArrayInputStream(fileContent);
                        InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                        try (CSVReader csvReader = new CSVReaderBuilder(inputStreamReader).withCSVParser(new RFC4180Parser()).build()) {
                            String[] header = csvReader.readNext();
                            String[] row;
                            while ((row = csvReader.readNext()) != null) {
                                // 将行数据转换为Java对象并进行处理
                                // 示例代码中使用了Arrays.toString()打印行数据
                                JSONObject message = new JSONObject();
                                for (int i = 0; i < header.length; i++) {
                                    message.put(header[i], i < row.length ? row[i] : "");
                                }
                                PullMessage<JSONObject> pullMessage = new PullMessage<>();
                                pullMessage.setMessage(message);
                                MessageOffset messageOffset = new MessageOffset(this.startTime + ";" + (pageNum + 10000));
                                messageOffset.addLayerOffset(0);
                                pullMessage.setMessageOffset(messageOffset);
                                msgs.add(pullMessage);
                            }
                        } catch (CsvValidationException e) {
                            LOGGER.error("CSV_Parse_Error", e);
                        }
                    }
                }
                return msgs;
            }

        };
    }

    protected String createTime(long time) {
        Date date = new Date(time);
        ZonedDateTime localDateTime = date.toInstant().atZone(ZoneId.systemDefault());
        return localDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }

    @Override
    public List<ISplit<?, ?>> fetchAllSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        long baseDataAdd = pollingMinute / splitCount;
        long remainder = pollingMinute % splitCount;
        Date date = DateUtil.getWindowBeginTime(DateUtil.parseTime(initFirstTime).getTime(), pollingMinute);
        for (int i = 0; i < splitCount; i++) {
            long dataAdd = baseDataAdd + (remainder > 0 ? 1 : 0);
            remainder--;
            Date endDate = DateUtil.addMinute(date, (int)dataAdd);
            LOGGER.info("[{}][{}] Get_Split_From({})_To({})", IdUtil.instanceId(), getName(), DateFormatUtils.format(date, "yyyy-MM-dd HH:mm:ss"), DateFormatUtils.format(endDate, "yyyy-MM-dd HH:mm:ss"));
            TencentOpenAPISplit openAPISplit = new TencentOpenAPISplit(getName() + "$#" + i, date.getTime(), dataAdd, pollingMinute);
            splits.add(openAPISplit);
            date = endDate;
        }
        return splits;
    }

    public String getEndPoint() {
        return endPoint;
    }

    public void setEndPoint(String endPoint) {
        this.endPoint = endPoint;
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

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

    public int getSleepTime() {
        return sleepTime;
    }

    public void setSleepTime(int sleepTime) {
        this.sleepTime = sleepTime;
    }
}
