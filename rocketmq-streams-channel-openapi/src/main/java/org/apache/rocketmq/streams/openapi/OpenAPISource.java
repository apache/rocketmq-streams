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
package org.apache.rocketmq.streams.openapi;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.profile.DefaultProfile;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.apache.rocketmq.streams.connectors.reader.AbstractSplitReader;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;
import org.apache.rocketmq.streams.openapi.executor.OpenAPIExecutor;
import org.apache.rocketmq.streams.openapi.splits.OpenAPISplit;
import org.apache.rocketmq.streams.sts.StsIdentity;
import org.apache.rocketmq.streams.sts.StsService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenAPISource extends AbstractPullSource {

    private static final Logger LOGGER = LoggerFactory.getLogger(OpenAPISource.class);

    protected String regionId;
    @ENVDependence
    protected String accessKeyId;
    @ENVDependence
    protected String accessSecret;
    protected String domain;
    protected String productName;
    protected String action;
    protected String version;
    protected String defaultParameterStr;//默认参数，格式是key;value,key:value

    protected transient Map<String, String> defaultParameters;

    protected String startTimeFieldName = "StartTime";//开始时间的字段名
    protected String endTimeFieldName = "EndTime";//结束时间的字段名
    protected String dataArrayFieldName;//返回的数据列表的字段名
    protected String pageNumberFieldName = "PageNumber";//取哪页数据
    protected String pageSizeFieldName = "PageSize";//每页数据最大值
    protected String totalCountField = "TotalCount";//总条数的字段名，支持嵌套

    protected int maxPageSize = 10;//最大一页数据数量
    protected boolean isTimeStamp = true;
    protected boolean isSecondTimeStamp = false;

    protected long dateAddOfHistoryData = 60;//如果有历史数据，则历史数据每次查询一个小时的数据

    protected String initFirstTime = DateUtil.getCurrentTimeString();

    protected Long pollingMinute = 10L;
    protected int splitCount = 1;//在一个周期内的并发性

    @ENVDependence private boolean isSts;
    private String aliuid;
    private String stsRoleArn;
    private String stsAssumeRoleFor;
    private String stsSessionPrefix = "openapi";

    private int stsExpireSeconds = 86400;

    private String ramEndpoint = "sts-inner.aliyuncs.com";

    private transient StsService stsService;

    @Override protected boolean initConfigurable() {
        if (StringUtil.isNotEmpty(this.defaultParameterStr)) {
            defaultParameters = new HashMap<>();
            String[] values = this.defaultParameterStr.split(",");
            for (String value : values) {
                String[] kv = value.split(":");
                defaultParameters.put(kv[0], kv[1]);
            }
        }
        if (StringUtil.isEmpty(stsRoleArn)) {
            stsRoleArn = "acs:ram::" + aliuid + ":role/aliyunserviceroleforsascloudsiem";
        }
        if (StringUtil.isEmpty(aliuid) && StringUtil.isNotEmpty(stsRoleArn)) {
            String tmp = stsRoleArn.replace("acs:ram::", "");
            int index = tmp.lastIndexOf(":");
            aliuid = tmp.substring(0, index);
        }
        stsAssumeRoleFor = aliuid;
        boolean b = initSts();
        return b && super.initConfigurable();
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        long baseDataAdd = pollingMinute / splitCount;
        long remainder = pollingMinute % splitCount;
        Date date = DateUtil.getWindowBeginTime(DateUtil.parseTime(initFirstTime).getTime(), pollingMinute);
        for (int i = 0; i < splitCount; i++) {
            long dataAdd = baseDataAdd + (remainder > 0 ? 1 : 0);
            remainder--;
            Date endDate = DateUtil.addMinute(date, (int) dataAdd);
            OpenAPISplit openAPISplit = new OpenAPISplit(i + "", date.getTime(), dataAdd, pollingMinute);
            splits.add(openAPISplit);
            date = endDate;
        }
        return splits;
    }

    @Override protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        return new AbstractSplitReader(split) {

            private long startTime;
            private long dateAdd;
            private long pollingMinute;
            private int pageNum = 1;
            private transient OpenAPIExecutor openAPIExecutor;

            @Override public void open(ISplit<?, ?> split) {
                OpenAPISplit openAPISplit = (OpenAPISplit) split;
                this.startTime = openAPISplit.getInitStartTime();
                this.dateAdd = openAPISplit.getDateAdd();
                this.pollingMinute = openAPISplit.getPollingMinute();
                this.openAPIExecutor = new OpenAPIExecutor(regionId, accessKeyId, accessSecret, domain, productName, action, version);
            }

            @Override public boolean next() {
                return startTime + pollingMinute * 60 * 1000 < System.currentTimeMillis();
            }

            @Override public List<PullMessage<?>> getMessage() {
                Map<String, Object> parameters = new HashMap<>();
                if (StringUtil.isNotEmpty(startTimeFieldName)) {
                    parameters.put(startTimeFieldName, createTime(startTime));
                }
                if (StringUtil.isNotEmpty(endTimeFieldName)) {
                    parameters.put(endTimeFieldName, createTime(startTime + dateAdd * 60 * 1000));
                }
                if (StringUtil.isNotEmpty(pageNumberFieldName)) {
                    parameters.put(pageNumberFieldName, pageNum);
                }
                if (StringUtil.isNotEmpty(pageSizeFieldName)) {
                    parameters.put(pageSizeFieldName, maxPageSize);
                }

                if (defaultParameters != null) {
                    parameters.putAll(defaultParameters);
                }
                DefaultProfile profile = null;
                if (isSts) {
                    try {
                        StsIdentity stsIdentity = stsService.getStsIdentity();
                        String localAccessId = stsIdentity.getAccessKeyId();
                        String localAccessKey = stsIdentity.getAccessKeySecret();
                        String localToken = stsIdentity.getSecurityToken();
                        profile = DefaultProfile.getProfile(regionId, localAccessId, localAccessKey, localToken);
                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException("sts execute error", e);
                    }

                }

                Object queryStartTime = parameters.get(startTimeFieldName);
                Object queryEndTime = parameters.get(endTimeFieldName);
                JSONObject msg = openAPIExecutor.invokeBySDK(parameters, profile);
                JSONArray data = msg.getJSONArray(dataArrayFieldName);
                if (data == null || data.size() == 0) {
                    startTime = startTime + pollingMinute * 60 * 1000;
                    pageNum = 1;
                    return null;
                } else if (StringUtil.isNotEmpty(totalCountField)) {
                    Integer totalCount = ReflectUtil.getBeanFieldOrJsonValue(msg, totalCountField);
                    if (totalCount == null) {
                        throw new RuntimeException("expect get totalCount from result msg by " + totalCountField + ", real is not find");
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
                msg.remove(dataArrayFieldName);
                LOGGER.info("[{}][{}] Execute_Openapi_Query_Result_Size({})_Query_Time_Gap({}-{})_Query_Page({})", IdUtil.instanceId(), getConfigureName(), data.size(), DateUtil.format(new Date(startTime)), DateUtil.format(new Date(startTime + dateAdd * 60 * 1000)), pageNum);
                for (int i = 0; i < data.size(); i++) {
                    JSONObject message = new JSONObject();
                    message.putAll(msg);
                    message.putAll(data.getJSONObject(i));
                    message.put(startTimeFieldName, queryStartTime);
                    message.put(endTimeFieldName, queryEndTime);

                    PullMessage pullMessage = new PullMessage();
                    pullMessage.setMessage(message);
                    MessageOffset messageOffset = new MessageOffset(startTime + ";" + (10000 + pageNum));
                    messageOffset.addLayerOffset(i);
                    pullMessage.setMessageOffset(messageOffset);
                    msgs.add(pullMessage);
                }
                return msgs;
            }

            @Override public long getDelay() {
                return System.currentTimeMillis() - startTime;
            }

            @Override public long getFetchedDelay() {
                return 0;
            }

            @Override public void seek(String cursor) {
                if (cursor == null) {
                    this.cursor = this.startTime + ";" + (pageNum + 10000);
                    return;
                }
                this.cursor = cursor;
                String[] values = cursor.split(";");
                this.startTime = Long.parseLong(values[0]);
                this.pageNum = Integer.parseInt(values[1]) - 10000;
            }

            @Override public String getProgress() {
                this.cursor = this.startTime + ";" + (pageNum + 10000);
                return this.cursor;
            }
        };
    }

    protected Object createTime(long time) {
        if (isTimeStamp || isSecondTimeStamp) {
            if (isSecondTimeStamp) {
                return time / 1000;
            }
            return time;
        } else {
            return DateUtil.longToString(time);
        }

    }

    private boolean initSts() {
        if (isSts) {
            if (StringUtil.isEmpty(stsRoleArn) || StringUtil.isEmpty(stsAssumeRoleFor)) {
                return false;
            }
            stsService = new StsService();
            stsService.setAccessId(accessKeyId);
            stsService.setAccessKey(accessSecret);
            stsService.setRamEndPoint(ramEndpoint);
            stsService.setStsExpireSeconds(stsExpireSeconds);
            stsService.setStsSessionPrefix(stsSessionPrefix);
            stsService.setRoleArn(stsRoleArn);
            stsService.setStsAssumeRoleFor(stsAssumeRoleFor);
        }
        return true;
    }

    public String getDefaultParameterStr() {
        return defaultParameterStr;
    }

    public void setDefaultParameterStr(String defaultParameterStr) {
        this.defaultParameterStr = defaultParameterStr;
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public String getAccessKeyId() {
        return accessKeyId;
    }

    public void setAccessKeyId(String accessKeyId) {
        this.accessKeyId = accessKeyId;
    }

    public String getAccessSecret() {
        return accessSecret;
    }

    public void setAccessSecret(String accessSecret) {
        this.accessSecret = accessSecret;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    @Override public String getVersion() {
        return version;
    }

    @Override public void setVersion(String version) {
        this.version = version;
    }

    public String getStartTimeFieldName() {
        return startTimeFieldName;
    }

    public void setStartTimeFieldName(String startTimeFieldName) {
        this.startTimeFieldName = startTimeFieldName;
    }

    public String getEndTiemFieldName() {
        return endTimeFieldName;
    }

    public void setEndTiemFieldName(String endTiemFieldName) {
        this.endTimeFieldName = endTiemFieldName;
    }

    public String getDataArrayFieldName() {
        return dataArrayFieldName;
    }

    public void setDataArrayFieldName(String dataArrayFieldName) {
        this.dataArrayFieldName = dataArrayFieldName;
    }

    public String getPageNumberFieldName() {
        return pageNumberFieldName;
    }

    public void setPageNumberFieldName(String pageNumberFieldName) {
        this.pageNumberFieldName = pageNumberFieldName;
    }

    public String getPageSizeFieldName() {
        return pageSizeFieldName;
    }

    public void setPageSizeFieldName(String pageSizeFieldName) {
        this.pageSizeFieldName = pageSizeFieldName;
    }

    public int getMaxPageSize() {
        return maxPageSize;
    }

    public void setMaxPageSize(int maxPageSize) {
        this.maxPageSize = maxPageSize;
    }

    public String getInitFirstTime() {
        return initFirstTime;
    }

    public boolean isTimeStamp() {
        return isTimeStamp;
    }

    public void setTimeStamp(boolean timeStamp) {
        isTimeStamp = timeStamp;
    }

    public void setInitFirstTime(String initFirstTime) {
        this.initFirstTime = initFirstTime;
    }

    public Long getPollingMinute() {
        return pollingMinute;
    }

    public void setPollingMinute(Long pollingMinute) {
        this.pollingMinute = pollingMinute;
    }

    public int getSplitCount() {
        return splitCount;
    }

    public void setSplitCount(int splitCount) {
        this.splitCount = splitCount;
    }

    public boolean isSecondTimeStamp() {
        return isSecondTimeStamp;
    }

    public void setSecondTimeStamp(boolean secondTimeStamp) {
        isSecondTimeStamp = secondTimeStamp;
    }

    public String getTotalCountField() {
        return totalCountField;
    }

    public void setTotalCountField(String totalCountField) {
        this.totalCountField = totalCountField;
    }

    public long getDateAddOfHistoryData() {
        return dateAddOfHistoryData;
    }

    public void setDateAddOfHistoryData(long dateAddOfHistoryData) {
        this.dateAddOfHistoryData = dateAddOfHistoryData;
    }

    public boolean isSts() {
        return isSts;
    }

    public void setSts(boolean sts) {
        isSts = sts;
    }

    public String getStsRoleArn() {
        return stsRoleArn;
    }

    public void setStsRoleArn(String stsRoleArn) {
        this.stsRoleArn = stsRoleArn;
    }

    public String getStsAssumeRoleFor() {
        return stsAssumeRoleFor;
    }

    public void setStsAssumeRoleFor(String stsAssumeRoleFor) {
        this.stsAssumeRoleFor = stsAssumeRoleFor;
    }

    public String getStsSessionPrefix() {
        return stsSessionPrefix;
    }

    public void setStsSessionPrefix(String stsSessionPrefix) {
        this.stsSessionPrefix = stsSessionPrefix;
    }

    public int getStsExpireSeconds() {
        return stsExpireSeconds;
    }

    public void setStsExpireSeconds(int stsExpireSeconds) {
        this.stsExpireSeconds = stsExpireSeconds;
    }

    public String getRamEndpoint() {
        return ramEndpoint;
    }

    public void setRamEndpoint(String ramEndpoint) {
        this.ramEndpoint = ramEndpoint;
    }

    public String getAliuid() {
        return aliuid;
    }

    public void setAliuid(String aliuid) {
        this.aliuid = aliuid;
    }
}
