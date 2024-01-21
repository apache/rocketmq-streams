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
package org.apache.rocketmq.streams.huawei;

import com.google.common.collect.Lists;
import com.obs.services.ObsClient;
import com.obs.services.exception.ObsException;
import com.obs.services.model.DownloadFileRequest;
import com.obs.services.model.DownloadFileResult;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.junit.Before;
import org.junit.Test;

public class ObsTest {

    String endPoint = "";
    String ak = "";
    String sk = "";

    private ObsClient obsClient;

    @Before
    public void init() {
        obsClient = new ObsClient(ak, sk, endPoint);
    }

    @Test
    public void test() {
        ListObjectsRequest request = new ListObjectsRequest("waf-alert");
        // 设置每页100个对象
        request.setPrefix("LogTanks/cn-east-3/2023/07/19/waf-project-test/vpc-flow-log-eip/");
        request.setMaxKeys(100);

        ObjectListing result;
        do {
            result = obsClient.listObjects(request);
            for (ObsObject obsObject : result.getObjects()) {
                System.out.println(obsObject.getBucketName() + "---" + obsObject.getObjectKey());
                ObjectMetadata metadata = obsClient.getObjectMetadata("waf-alert", obsObject.getObjectKey());
                System.out.println("\t" + metadata.getContentType());
                System.out.println("\t" + metadata.getContentLength());
                System.out.println("\t" + metadata.getUserMetadata("property"));
                System.out.println("\t" + metadata.getOriginalHeaders());

                //                getGzipContent(obsClient.getObject(obsObject.getBucketName(), obsObject.getObjectKey()));
            }
            request.setMarker(result.getNextMarker());
        }
        while (result.isTruncated());

    }

    private void download(ObsObject obsObject) {
        DownloadFileRequest request = new DownloadFileRequest("waf-alert", obsObject.getObjectKey());
        request.setDownloadFile("/Users/junjie.cheng/tmp/" + obsObject.getObjectKey());
        request.setTaskNum(5);
        request.setPartSize(10 * 1024 * 1024);
        request.setEnableCheckpoint(true);
        try {
            // 进行断点续传下载
            DownloadFileResult result = obsClient.downloadFile(request);
            System.out.println("Etag:" + result.getObjectMetadata().getEtag());
        } catch (ObsException e) {
            System.out.println("Error Code:" + e.getErrorCode());
            System.out.println("Error Message: " + e.getErrorMessage());
        }
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] getGzipContent(ObsObject obsObject) {
        // 读取对象内容
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        GzipCompressorInputStream gzIn = null;
        try {
            gzIn = new GzipCompressorInputStream(obsObject.getObjectContent());
            byte[] b = new byte[1024];
            int len;
            while ((len = gzIn.read(b)) != -1) {
                bos.write(b, 0, len);
            }

            // 将CSV数据解析为Java对象
            ByteArrayInputStream fileInputStream = new ByteArrayInputStream(bos.toByteArray());
            InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
            BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
            String line = bufferedReader.readLine();
            while (line != null) {
                System.out.println(line);
                line = bufferedReader.readLine();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                bos.close();
                if (gzIn != null) {
                    gzIn.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return new byte[0];
    }

}