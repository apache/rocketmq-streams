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

import com.opencsv.CSVReader;
import com.tencentcloudapi.common.Credential;
import com.tencentcloudapi.common.exception.TencentCloudSDKException;
import com.tencentcloudapi.common.profile.ClientProfile;
import com.tencentcloudapi.common.profile.HttpProfile;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.rocketmq.streams.tencent.TencentOpenAPIClient;
import org.junit.Before;
import org.junit.Test;

public class TestCommonAPI {

    private Credential cred;

    @Before
    public void init() {
        // 实例化一个认证对象，入参需要传入腾讯云账户 SecretId，SecretKey。
        // 为了保护密钥安全，建议将密钥设置在环境变量中或者配置文件中，请参考本文凭证管理章节。
        // 硬编码密钥到代码中有可能随代码泄露而暴露，有安全隐患，并不推荐。
        // Credential cred = new Credential("SecretId", "SecretKey");
        this.cred = new Credential("", "");

    }

    @Test
    public void test() {

        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的

        String endpoint = "waf.tencentcloudapi.com";
        String version = "2018-01-25";
        TencentOpenAPIClient client = new TencentOpenAPIClient(endpoint, version, cred, "ap-guangzhou", clientProfile) {
        };

        try {
            String rspStr = client.call("GetAttackDownloadRecords", "{}");
            System.out.println(rspStr);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test1() {

        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的

        String endpoint = "waf.tencentcloudapi.com";
        String version = "2018-01-25";
        TencentOpenAPIClient client = new TencentOpenAPIClient(endpoint, version, cred, "ap-guangzhou", clientProfile) {
        };

        try {

            String rspStr = client.call("PostAttackDownloadTask", "{\n"
                + "    \"EndTime\": \"2023-03-27T10:55:09+08:00\",\n"
                + "    \"TaskName\": \"test$#0_1679882628000\",\n"
                + "    \"QueryString\": \"method:GET\",\n"
                + "    \"StartTime\": \"2023-03-27T00:45:09+08:00\",\n"
                + "    \"Sort\": \"desc\",\n"
                + "    \"Domain\": \"all\"\n"
                + "}");

            System.out.println(rspStr);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test2() {
        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的

        String endpoint = "waf.tencentcloudapi.com";
        String version = "2018-01-25";
        TencentOpenAPIClient client = new TencentOpenAPIClient(endpoint, version, cred, "ap-guangzhou", clientProfile) {
        };

        try {
            String rspStr = client.call("DescribeRuleLimit", "{\n"
                + "    \"Domain\": \"leixi6.aliyundemo.com\"\n"
                + "}");
            System.out.println(rspStr);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test3() {
        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的

        String endpoint = "waf.tencentcloudapi.com";
        String version = "2018-01-25";
        TencentOpenAPIClient client = new TencentOpenAPIClient(endpoint, version, cred, "ap-guangzhou", clientProfile) {
        };

        try {
            String rspStr = client.call("DescribeAccessExports", "{\n"
                + "    \"TopicId\": \"1ae37c76-df99-4e2b-998c-20f39eba6226\"\n"
                + "}");
            System.out.println(rspStr);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test4() {
        Date date = new Date(System.currentTimeMillis());
        ZonedDateTime localDateTime = date.toInstant().atZone(ZoneId.systemDefault());
        String dateFormatter = localDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        System.out.println(dateFormatter);

    }

    @Test
    public void test6() {

        // 实例化一个http选项，可选的，没有特殊需求可以跳过
        HttpProfile httpProfile = new HttpProfile();
        // 实例化一个client选项，可选的，没有特殊需求可以跳过
        ClientProfile clientProfile = new ClientProfile();
        clientProfile.setHttpProfile(httpProfile);
        // 实例化要请求产品的client对象,clientProfile是可选的

        String endpoint = "waf.tencentcloudapi.com";
        String version = "2018-01-25";
        TencentOpenAPIClient client = new TencentOpenAPIClient(endpoint, version, cred, "ap-guangzhou", clientProfile) {
        };

        try {
            String rspStr = client.call("DescribeWafAutoDenyStatus", "{}");
            System.out.println(rspStr);
        } catch (TencentCloudSDKException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void test5() {
        try {
            String fileUrl = "";
            int BUFFER_SIZE = 10240;
            byte[] tarData;
            try (InputStream inputStream = new URL(fileUrl).openStream()) {
                ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
                byte[] buffer = new byte[BUFFER_SIZE];
                int n;
                while ((n = inputStream.read(buffer, 0, BUFFER_SIZE)) >= 0) {
                    outputStream.write(buffer, 0, n);
                }
                outputStream.close();
                tarData = outputStream.toByteArray();
            }

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
                    CSVReader csvReader = new CSVReader(inputStreamReader);
                    String[] header = csvReader.readNext();

                    String[] row;
                    while ((row = csvReader.readNext()) != null) {
                        // 将行数据转换为Java对象并进行处理
                        // 示例代码中使用了Arrays.toString()打印行数据
                        System.out.println(Arrays.toString(row));
                    }
                    csvReader.close();
                }
            }
        } catch (Exception e) {

        }
    }

}
