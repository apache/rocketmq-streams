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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.openapi.executor.OpenAPIExecutor;
import org.apache.rocketmq.streams.sts.StsIdentity;
import org.apache.rocketmq.streams.sts.StsService;

public class OpenAPIDim extends BasedConfigurable implements IDim {

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

    protected String dataArrayFieldName;//返回的数据列表的字段名

    @ENVDependence private boolean isSts;
    private String aliuid;
    private String stsRoleArn;
    private String stsAssumeRoleFor;
    private String stsSessionPrefix = "openapi";

    private int stsExpireSeconds = 86400;

    private String ramEndpoint = "sts-inner.aliyuncs.com";

    private transient StsService stsService;

    protected transient OpenAPIExecutor openAPIExecutor;

    public OpenAPIDim() {
        setType(TYPE);
    }

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

        stsAssumeRoleFor = aliuid;
        boolean b = initSts();
        this.openAPIExecutor = new OpenAPIExecutor(regionId, accessKeyId, accessSecret, domain, productName, action, version);
        return b && super.initConfigurable();
    }

    @Override public List<Map<String, Object>> matchExpression(String expressionStr, JSONObject msg, boolean needAll,
        String script) {
        Map<String, Object> parameters = new HashMap<>();
        if (defaultParameters != null) {
            for (String key : defaultParameters.keySet()) {
                Object value = SQLUtil.parseIbatisSQL(msg, defaultParameters.get(key));
                parameters.put(key, value);
            }
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
        JSONObject jsonObject = openAPIExecutor.invokeBySDK(parameters, profile);
        if (StringUtil.isEmpty(dataArrayFieldName)) {
            List<Map<String, Object>> list = new ArrayList<>();
            list.add(jsonObject);
            return list;
        } else {
            JSONArray data = msg.getJSONArray(dataArrayFieldName);
            if (data == null) {
                List<Map<String, Object>> list = new ArrayList<>();
                list.add(jsonObject);
                return list;
            }
            List<Map<String, Object>> msgs = new ArrayList<>();
            msg.remove(dataArrayFieldName);

            for (int i = 0; i < data.size(); i++) {
                JSONObject message = new JSONObject();
                message.putAll(msg);
                message.putAll(data.getJSONObject(i));
                msgs.add(message);
            }
            return msgs;
        }
    }

    @Override public Map<String, Object> matchExpression(String expressionStr, JSONObject msg) {
        List<Map<String, Object>> rows = matchExpression(expressionStr, msg, true, null);
        if (rows != null && rows.size() > 0) {
            return rows.get(0);
        }
        return null;
    }

    @Override public List<String> getIndexs() {
        return new ArrayList<>();
    }

    @Override public String addIndex(String... indexs) {
        return null;
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

    public String getDefaultParameterStr() {
        return defaultParameterStr;
    }

    public void setDefaultParameterStr(String defaultParameterStr) {
        this.defaultParameterStr = defaultParameterStr;
    }

    public Map<String, String> getDefaultParameters() {
        return defaultParameters;
    }

    public void setDefaultParameters(Map<String, String> defaultParameters) {
        this.defaultParameters = defaultParameters;
    }

    public String getDataArrayFieldName() {
        return dataArrayFieldName;
    }

    public void setDataArrayFieldName(String dataArrayFieldName) {
        this.dataArrayFieldName = dataArrayFieldName;
    }

    public boolean isSts() {
        return isSts;
    }

    public void setSts(boolean sts) {
        isSts = sts;
    }

    public String getAliuid() {
        return aliuid;
    }

    public void setAliuid(String aliuid) {
        this.aliuid = aliuid;
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
}
