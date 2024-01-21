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
package org.apache.rocketmq.streams.openapi.executor;

import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.http.ProtocolType;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.utils.SQLUtil;

public abstract class AbstractOpenAPIExecutor {
    public static final String JSON_FORMAT = "json";
    public static final String XML_FORMAT = "xml";
    private static final String PREFIX = "dipper.upgrade.channel.openapi.envkey";
    /**
     * 需要用户输入的参数
     */
    protected String productName;//业务名称
    //调用业务接口的名称方法参照《OpenAPI变量的获取和例子项目的使用》中2.1.2节的方法获取
    protected String action;
    //    protected JSONObject query = new JSONObject();//业务参数
    //具体要调用openapi 接口 的版本 sas的版本如下 其他服务参考 对应服务的openapi文档
    protected String version = "2018-01-16";
    /**
     * 如果作为channel，需要配置的调度参数
     */
    protected Long pollingTime = 10 * 60L;//轮询时间，单位是秒
    protected String cron;//详细的定时配置
    protected boolean startNow = true;//启动立刻执行一次
    /**
     * 需要预先配置的参数
     */
    @ENVDependence
    protected String accessKeyId;
    @ENVDependence
    protected String accessSecret;
    //String domain = "sasprivate.cn-qingdao-env12-d01.env12.shuguang.com";//获取方法参照《OpenAPI变量的获取和例子项目的使用》中1.2节的方法获取
    @ENVDependence
    protected String domain;
    /**
     * 默认不需要改变，特殊情况，可以覆盖的参数
     */
    protected String method = "GET"; //默认是get请求
    protected String format = JSON_FORMAT;//返回响应的格式  取值为JSON|XML  默认是XML
    protected String protocol = ProtocolType.HTTPS.toString();
    protected int maxRetryCounts = 3;//最大重试次数
    protected transient Map<String, Object> value2Enum = new HashMap<>();

    public AbstractOpenAPIExecutor() {
        value2Enum.put(ProtocolType.HTTP.toString(), ProtocolType.HTTP);
        value2Enum.put(ProtocolType.HTTPS.toString(), ProtocolType.HTTPS);

        value2Enum.put(MethodType.GET.toString(), MethodType.GET);
        value2Enum.put(MethodType.POST.toString(), MethodType.POST);

        value2Enum.put(JSON_FORMAT, FormatType.JSON);
        value2Enum.put(XML_FORMAT, FormatType.XML);
    }

    protected JSONObject appBusinessParameters(JSONObject query) {
        JSONObject paras = new JSONObject();
        if (query == null || query.size() == 0) {
            return paras;
        }

        Iterator<Map.Entry<String, Object>> it = query.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String key = entry.getKey();
            Object value = entry.getValue();
            value = SQLUtil.parseIbatisSQL(query, value.toString(), true);
            value = value.toString().replaceAll("'", "");
            paras.put(key, value);
        }
        return paras;
    }

    protected JSONObject doInvoke(JSONObject query) {
        JSONObject paras = appBusinessParameters(query);
        JSONObject jsonObject = invoke(paras);
        return jsonObject;
    }

    protected abstract JSONObject invoke(JSONObject paras);

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public boolean isStartNow() {
        return startNow;
    }

    public void setStartNow(boolean startNow) {
        this.startNow = startNow;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
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

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    public String getFormat() {
        return format;
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public int getMaxRetryCounts() {
        return maxRetryCounts;
    }

    public void setMaxRetryCounts(int maxRetryCounts) {
        this.maxRetryCounts = maxRetryCounts;
    }

    public Long getPollingTime() {
        return pollingTime;
    }

    public void setPollingTime(Long pollingTime) {
        this.pollingTime = pollingTime;
    }

    protected void destroyCunsumer() {
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }
}
