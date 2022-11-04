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
package org.apache.rocketmq.streams.sts;

import com.aliyuncs.DefaultAcsClient;
import com.aliyuncs.exceptions.ClientException;
import com.aliyuncs.http.MethodType;
import com.aliyuncs.profile.DefaultProfile;
import com.aliyuncs.profile.IClientProfile;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityRequest;
import com.aliyuncs.sts.model.v20150401.AssumeRoleWithServiceIdentityResponse;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StsService {

    private static final Logger logger = LoggerFactory.getLogger(StsService.class);

    public static final String STS_ACCESS_ID = "stsAccessId";
    public static final String STS_ACCESS_KEY = "stsAccessKey";
    public static final String STS_ROLE_ARN = "stsRoleArn";
    public static final String STS_ASSUME_ROLE_FOR = "stsAssumeRoleFor";
    public static final String STS_TIME_OUT_SECOND = "stsTimeoutSecond";
    public static final String STS_RAM_ENDPOINT = "ramEndpoint";
    public static final String STS_SESSION_NAME_PREFIX = "stsSessionPrefix";
    private static final String CACHE_STS_KEY = "stsCacheKey";
    private static final String DEFAULT_RAM_ENDPOINT = "sts-inner.aliyuncs.com";
    private static final int DEFAULT_STS_EXPIRE_SECOND = 86400;

    String accessId;
    String accessKey;
    String roleArn;
    String ramEndPoint;
    String stsSessionPrefix;
    String stsAssumeRoleFor;
    int stsExpireSeconds;
    String stsSessionName;
    LoadingCache<String, StsIdentity> cacheLoader;

    public StsService() {

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

    public String getRoleArn() {
        return roleArn;
    }

    public void setRoleArn(String roleArn) {
        this.roleArn = roleArn;
    }

    public String getRamEndPoint() {
        return ramEndPoint;
    }

    public void setRamEndPoint(String ramEndPoint) {
        this.ramEndPoint = ramEndPoint;
    }

    public String getStsSessionPrefix() {
        return stsSessionPrefix;
    }

    public void setStsSessionPrefix(String stsSessionPrefix) {
        this.stsSessionPrefix = stsSessionPrefix;
    }

    public String getStsAssumeRoleFor() {
        return stsAssumeRoleFor;
    }

    public void setStsAssumeRoleFor(String stsAssumeRoleFor) {
        this.stsAssumeRoleFor = stsAssumeRoleFor;
    }

    public int getStsExpireSeconds() {
        return stsExpireSeconds;
    }

    public void setStsExpireSeconds(int stsExpireSeconds) {
        this.stsExpireSeconds = stsExpireSeconds;
    }

    public StsService(Properties properties) {
        this.accessId = CheckParametersUtils.checkIfNullThrowException(properties.getProperty(STS_ACCESS_ID), STS_ACCESS_ID + " is null, pls check params");
        this.accessKey = CheckParametersUtils.checkIfNullThrowException(properties.getProperty(STS_ACCESS_KEY), STS_ACCESS_KEY + " is null, pls check params");
        this.roleArn = CheckParametersUtils.checkIfNullThrowException(properties.getProperty(STS_ROLE_ARN), STS_ROLE_ARN + " is null, pls check params");
        this.stsAssumeRoleFor = CheckParametersUtils.checkIfNullThrowException(properties.getProperty(STS_ASSUME_ROLE_FOR), STS_ASSUME_ROLE_FOR + " is null, pls check params");
        this.ramEndPoint = CheckParametersUtils.checkIfNullReturnDefault(properties.getProperty(STS_RAM_ENDPOINT), DEFAULT_RAM_ENDPOINT);
        this.stsSessionPrefix = CheckParametersUtils.checkIfNullReturnDefault(properties.getProperty(STS_SESSION_NAME_PREFIX), "DEFAULT_SESSION_NAME");
        this.stsExpireSeconds = CheckParametersUtils.checkIfNullReturnDefaultInt(properties.get(STS_TIME_OUT_SECOND), DEFAULT_STS_EXPIRE_SECOND);
        createCacheLoader();
    }

    public int getRefreshTimeSecond() {
        return stsExpireSeconds / 2;
    }

    public StsIdentity getStsIdentity() throws ExecutionException {
        if (cacheLoader == null) {
            createCacheLoader();
        }
        return cacheLoader.get(CACHE_STS_KEY);
    }

    private final void createCacheLoader() {
        this.cacheLoader = CacheBuilder.newBuilder().concurrencyLevel(5).initialCapacity(1).maximumSize(3).expireAfterWrite(getRefreshTimeSecond(), TimeUnit.SECONDS).build(new StsIdentityCacheLoader());
    }

    private DefaultAcsClient getRamClient() throws ClientException {
        DefaultProfile.addEndpoint("", "", "Sts", ramEndPoint);
        // 构造default profile（参数留空，无需添加region ID）
        IClientProfile profile = DefaultProfile.getProfile("", accessId, accessKey);
        // 用profile构造client
        return new DefaultAcsClient(profile);
    }

    private final String refreshSessionName() {
        this.stsSessionName = stsSessionPrefix + "_" + System.currentTimeMillis();
        return stsSessionName;
    }

    class StsIdentityCacheLoader extends CacheLoader<String, StsIdentity> {

        @Override public StsIdentity load(String s) throws ClientException {
            DefaultAcsClient client = getRamClient();
            final AssumeRoleWithServiceIdentityRequest request = new AssumeRoleWithServiceIdentityRequest();
            request.setMethod(MethodType.POST);
            request.setRoleArn(roleArn);
            request.setRoleSessionName(refreshSessionName());
            request.setAssumeRoleFor(stsAssumeRoleFor);
            request.setDurationSeconds((long) stsExpireSeconds);
            final AssumeRoleWithServiceIdentityResponse response = client.getAcsResponse(request);
            StsIdentity stsIdentity = new StsIdentity(response.getCredentials().getAccessKeyId(), response.getCredentials().getAccessKeySecret(), response.getCredentials().getSecurityToken(), response.getCredentials().getExpiration());
            logger.info("CacheLoader refresh stsToken success, accessId is {}. ", stsIdentity.getAccessKeyId());
            return stsIdentity;
        }
    }
}
