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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.IAcsClient;
import com.aliyuncs.http.FormatType;
import com.aliyuncs.profile.DefaultProfile;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;

public class OpenAPIExecutor extends HttpOpenAPIExecutor {
    @ENVDependence
    protected String regionId = "global:region";
    protected boolean userHttp = false;

    public OpenAPIExecutor(String regionId, String accessKeyId, String accessSecret, String domain, String productName, String action, String version) {
        this.setRegionId(regionId);
        this.setAccessKeyId(accessKeyId);
        this.setAccessSecret(accessSecret);
        this.setAction(action);
        this.setDomain(domain);
        this.setProductName(productName);
        this.setVersion(version);
    }

    @Override
    protected JSONObject invoke(JSONObject businessParameters) {
        if (userHttp) {
            return super.invoke(businessParameters);
        } else {
            return invokeBySDK(businessParameters, null);
        }
    }

    public JSONObject invokeBySDK(Map<String, Object> paras, DefaultProfile profile) {
        try {
            if (profile == null) {
                profile = DefaultProfile.getProfile(this.regionId, this.accessKeyId, this.accessSecret);
            }
            DefaultProfile.addEndpoint(regionId, regionId, productName, domain);
            IAcsClient client = new DefaultAcsClient(profile);
            GenericRpcAcsRequest request = new GenericRpcAcsRequest(productName, version, action);
            request.setAcceptFormat((FormatType) value2Enum.get(format));
            request.setConnectTimeout(80000);
            request.setReadTimeout(80000);
            if (paras != null && paras.size() > 0) {
                for (Map.Entry<String, Object> entry : paras.entrySet()) {
                    request.putQueryParameter(entry.getKey(), entry.getValue());
                }
            }
            if (domain == null) {
                throw new RuntimeException("product not exist");
            }
            if (domain.contains("[")) {
                domain = domain.replaceAll("\\[[^]]+]", regionId);
            }
            GenericAcsResponse response = client.getAcsResponse(request, false, maxRetryCounts);
            return JSON.parseObject(response.getContent());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("invoke pop service error " + productName + ":" + action, e);
        }
    }

    public String getRegionId() {
        return regionId;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public boolean isUserHttp() {
        return userHttp;
    }

    public void setUserHttp(boolean userHttp) {
        this.userHttp = userHttp;
    }
}
