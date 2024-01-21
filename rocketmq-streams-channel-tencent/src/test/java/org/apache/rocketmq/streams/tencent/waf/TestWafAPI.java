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

import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;
import com.tencentcloudapi.waf.v20180125.WafClient;
import com.tencentcloudapi.waf.v20180125.models.GetAttackDownloadRecordsRequest;
import com.tencentcloudapi.waf.v20180125.models.GetAttackDownloadRecordsResponse;
import com.tencentcloudapi.waf.v20180125.models.PostAttackDownloadTaskRequest;
import com.tencentcloudapi.waf.v20180125.models.PostAttackDownloadTaskResponse;
import org.junit.Before;
import org.junit.Test;

public class TestWafAPI {

    private WafClient client;

    @Before
    public void init() {
        // 实例化一个认证对象，入参需要传入腾讯云账户 SecretId，SecretKey。
        // 为了保护密钥安全，建议将密钥设置在环境变量中或者配置文件中，请参考本文凭证管理章节。
        // 硬编码密钥到代码中有可能随代码泄露而暴露，有安全隐患，并不推荐。
        // Credential cred = new Credential("SecretId", "SecretKey");
        Credential cred = new Credential("", "");
        //Credential cred = new Credential("", "");
        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        httpProfile.setEndpoint("waf.tencentcloudapi.com");
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的
        client = new WafClient(cred, "ap-guangzhou", clientProfile);

    }

    @Test
    public void testWafGetAttackDownloadRecords() {
        try {
            // 实例化一个请求对象,每个接口都会对应一个request对象
            GetAttackDownloadRecordsRequest req = new GetAttackDownloadRecordsRequest();

            // 返回的resp是一个GetAttackDownloadRecordsResponse的实例，与请求对象对应
            GetAttackDownloadRecordsResponse resp = client.GetAttackDownloadRecords(req);
            // 输出json格式的字符串回包
            System.out.println(GetAttackDownloadRecordsResponse.toJsonString(resp));
        } catch (TencentCloudSDKException e) {
            System.out.println(e.toString());
        }
    }

    @Test
    public void testPostAttackDownloadTask() {
        PostAttackDownloadTaskRequest postAttackDownloadTaskRequest = new PostAttackDownloadTaskRequest();
        postAttackDownloadTaskRequest.setDomain("all");
        postAttackDownloadTaskRequest.setStartTime("2023-09-01T10:20:00+08:00");
        postAttackDownloadTaskRequest.setEndTime("2023-09-01T11:10:00+08:00");
        postAttackDownloadTaskRequest.setQueryString("method:*");
        postAttackDownloadTaskRequest.setTaskName("test_waf_download_1");

        PostAttackDownloadTaskResponse postAttackDownloadTaskResponse = null;
        try {
            postAttackDownloadTaskResponse = client.PostAttackDownloadTask(postAttackDownloadTaskRequest);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
        System.out.println(PostAttackDownloadTaskResponse.toJsonString(postAttackDownloadTaskResponse));
    }

}
