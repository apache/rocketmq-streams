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
package org.apache.rocketmq.streams.tencent.common;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.tencent.TencentOpenAPIClient;
import org.apache.rocketmq.streams.tencent.TencentOpenAPISplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TencentOpenAPISource extends AbstractPullSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(TencentOpenAPISource.class);

    protected String endPoint;

    protected String accessId;

    protected String accessKey;

    protected String version;

    protected String action;

    protected String region;

    protected String jsonPayLoad;

    protected String startField;

    protected String endField;

    protected String pageNumberField;

    protected String pageSizeField;

    protected String totalField;

    protected String dataField;

    protected String requestIdField;

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

    /**
     * 最大一页数据数量
     */
    protected int maxPageSize = 10;

    /**
     * 时间戳是毫秒还是秒， true是毫秒，false是秒
     */
    protected boolean isTimeStamp = true;

    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    @Override
    protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        return new AbstractSplitReader(split) {

            private long startTime;
            private long dateAdd;
            private long pollingMinute;
            private int pageNum = 1;
            private transient TencentOpenAPIClient tencentOpenAPIClient;

            @Override
            public void open() {
                Credential cred = new Credential(accessId, accessKey);
                TencentOpenAPISplit openAPISplit = (TencentOpenAPISplit) split;
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
                Map<String, Object> parameters = new HashMap<>();
                if (StringUtil.isNotEmpty(jsonPayLoad)) {
                    parameters = JSONObject.parseObject(jsonPayLoad);
                }

                if (StringUtil.isNotEmpty(startField)) {
                    parameters.put(startField, createTime(startTime));
                }
                if (StringUtil.isNotEmpty(endField)) {
                    parameters.put(endField, createTime(startTime + dateAdd * 60 * 1000));
                }
                if (StringUtil.isNotEmpty(pageNumberField)) {
                    parameters.put(pageNumberField, pageNum);
                }
                if (StringUtil.isNotEmpty(pageSizeField)) {
                    parameters.put(pageSizeField, maxPageSize);
                }
                Object queryStartTime = parameters.get(startField);
                Object queryEndTime = parameters.get(endField);
                JSONObject msg = null;
                try {
                    String resultString = tencentOpenAPIClient.call(action, JSONObject.toJSONString(parameters));
                    JSONObject result = JSONObject.parseObject(resultString);
                    msg = result == null ? new JSONObject() : result.getJSONObject("Response");
                } catch (TencentCloudSDKException e) {
                    throw new RuntimeException(e);
                }
                JSONArray data = msg.getJSONArray(dataField);
                if (data == null || data.isEmpty()) {
                    startTime = startTime + pollingMinute * 60 * 1000;
                    pageNum = 1;
                    return null;
                } else if (StringUtil.isNotEmpty(totalField)) {
                    Integer totalCount = ReflectUtil.getBeanFieldOrJsonValue(data, totalField);
                    if (totalCount == null) {
                        throw new RuntimeException("expect get totalCount from result msg by " + totalField + ", real is not find");
                    }
                    long currentCount = (long) (pageNum) * maxPageSize;
                    if (currentCount >= totalCount) {
                        startTime = startTime + pollingMinute * 60 * 1000;
                        pageNum = 1;
                    } else {
                        pageNum++;
                    }
                } else if (data.size() < maxPageSize) {
                    startTime = startTime + pollingMinute * 60 * 1000;
                    pageNum = 1;
                } else {
                    pageNum++;
                }
                List<PullMessage<?>> msgs = new ArrayList<>();
                msg.remove(dataField);
                LOGGER.info("[{}][{}] Execute_Openapi_Query_Result_Size({})_Query_Time_Gap({}-{})_Query_Page({})", IdUtil.instanceId(), getName(), data.size(), DateUtil.format(new Date(startTime)), DateUtil.format(new Date(startTime + dateAdd * 60 * 1000)), pageNum);
                for (int i = 0; i < data.size(); i++) {
                    JSONObject message = new JSONObject();
                    message.putAll(msg);
                    message.putAll(data.getJSONObject(i));
                    message.put(startField, queryStartTime);
                    message.put(endField, queryEndTime);
                    message.put(requestIdField, msg.get(requestIdField));

                    PullMessage<JSONObject> pullMessage = new PullMessage<>();
                    pullMessage.setMessage(message);
                    MessageOffset messageOffset = new MessageOffset(startTime + ";" + (10000 + pageNum));
                    messageOffset.addLayerOffset(i);
                    pullMessage.setMessageOffset(messageOffset);
                    msgs.add(pullMessage);
                }
                return msgs.iterator();
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

        };
    }

    protected Object createTime(long time) {
        if (isTimeStamp) {
            return time;
        } else {
            return time / 1000;
        }
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
            Date endDate = DateUtil.addMinute(date, (int) dataAdd);
            TencentOpenAPISplit openAPISplit = new TencentOpenAPISplit(i + "", date.getTime(), dataAdd, pollingMinute);
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

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getRegion() {
        return region;
    }

    public void setRegion(String region) {
        this.region = region;
    }

    public String getJsonPayLoad() {
        return jsonPayLoad;
    }

    public void setJsonPayLoad(String jsonPayLoad) {
        this.jsonPayLoad = jsonPayLoad;
    }

    public String getStartField() {
        return startField;
    }

    public void setStartField(String startField) {
        this.startField = startField;
    }

    public String getEndField() {
        return endField;
    }

    public void setEndField(String endField) {
        this.endField = endField;
    }

    public String getPageNumberField() {
        return pageNumberField;
    }

    public void setPageNumberField(String pageNumberField) {
        this.pageNumberField = pageNumberField;
    }

    public String getPageSizeField() {
        return pageSizeField;
    }

    public void setPageSizeField(String pageSizeField) {
        this.pageSizeField = pageSizeField;
    }

    public String getTotalField() {
        return totalField;
    }

    public void setTotalField(String totalField) {
        this.totalField = totalField;
    }

    public String getDataField() {
        return dataField;
    }

    public void setDataField(String dataField) {
        this.dataField = dataField;
    }

    public String getRequestIdField() {
        return requestIdField;
    }

    public void setRequestIdField(String requestIdField) {
        this.requestIdField = requestIdField;
    }
}
