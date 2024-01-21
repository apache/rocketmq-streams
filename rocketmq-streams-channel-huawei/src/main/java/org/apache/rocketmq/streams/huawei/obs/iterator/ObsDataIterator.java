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
package org.apache.rocketmq.streams.huawei.obs.iterator;

import com.alibaba.fastjson.JSONObject;

import com.google.common.collect.Lists;
import com.obs.services.ObsClient;
import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObsObject;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.zip.ZipInputStream;

import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.connectors.model.PullMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Obs数据迭代器
 */
public class ObsDataIterator implements Iterator<PullMessage<?>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ObsDataIterator.class);

    private final String fileType;
    private final MetaData metaData;
    private final String fieldDelimiter;
    private final String partitionPath;
    private final FinishedCallback callback;
    List<BufferedReader> bufferedReaderList = Lists.newArrayList();
    private String nextLine = null;
    private int hasNextIndex;

    public ObsDataIterator(ObsClient obsClient, String bucketName, String partitionPath, String fileType, String compressType, String fieldDelimiter, MetaData metaData, FinishedCallback callback) {
        this.fileType = fileType;
        this.metaData = metaData;
        this.fieldDelimiter = fieldDelimiter;
        this.partitionPath = partitionPath;
        this.callback = callback;

        ListObjectsRequest request = new ListObjectsRequest(bucketName);
        // 设置每页100个对象
        request.setPrefix(partitionPath);
        request.setMaxKeys(100);
        ObjectListing result = null;
        do {
            try {
                result = obsClient.listObjects(request);
                List<ObsObject> resultObjects = result.getObjects();
                if (!resultObjects.isEmpty()) {
                    LOGGER.info("Get_Obs_Objects_Success: {} | {} : ", bucketName, partitionPath);
                    for (ObsObject obsObject : resultObjects) {
                        ObsObject obsTemp = obsClient.getObject(obsObject.getBucketName(), obsObject.getObjectKey());
                        if (compressType.equalsIgnoreCase("gzip")) {
                            bufferedReaderList.add(getGzipContent(obsTemp));
                        } else {
                            bufferedReaderList.add(getZipContent(obsTemp));
                        }
                    }
                }
                request.setMarker(result.getNextMarker());
            } catch (Exception e) {
                LOGGER.error("Get_Obs_Objects_Error", e);
            }
        }
        while (result != null && result.isTruncated());
        try {
            for (hasNextIndex = 0; hasNextIndex < this.bufferedReaderList.size(); hasNextIndex++) {
                this.nextLine = this.bufferedReaderList.get(hasNextIndex).readLine();
                if (this.nextLine != null) {
                    break;
                }
            }
        } catch (IOException e) {
            LOGGER.error("Read_Obs_File_Error", e);
        }
    }

    @Override
    public boolean hasNext() {
        if (this.nextLine != null) {
            return true;
        } else {
            try {
                for (; hasNextIndex < this.bufferedReaderList.size(); hasNextIndex++) {
                    this.nextLine = this.bufferedReaderList.get(hasNextIndex).readLine();
                    if (this.nextLine != null) {
                        break;
                    }
                }
                if (this.nextLine != null) {
                    return true;
                } else {
                    close();
                    return false;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public PullMessage<?> next() {
        if (this.nextLine != null || hasNext()) {
            String line = this.nextLine;
            this.nextLine = null;
            JSONObject msg;
            if (fileType.equalsIgnoreCase("json")) {
                msg = JSONObject.parseObject(line);
            } else { //原始数据
                List<MetaDataField<?>> dataFields = metaData.getMetaDataFields();
                String[] data = line.split(fieldDelimiter);
                msg = new JSONObject();
                for (int i = 0; i < dataFields.size(); i++) {
                    MetaDataField<?> metaDataField = dataFields.get(i);
                    msg.put(metaDataField.getFieldName(), i < data.length ? data[i] : "");
                }
            }
            MessageOffset messageOffset = new MessageOffset(this.partitionPath);
            messageOffset.addLayerOffset(0);
            PullMessage<JSONObject> pullMessage = new PullMessage<>();
            pullMessage.setMessage(msg);
            pullMessage.setMessageOffset(messageOffset);
            return pullMessage;
        } else {
            throw new NoSuchElementException();
        }
    }

    private void close() {
        try {
            for (BufferedReader bufferedReader : bufferedReaderList) {
                bufferedReader.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            this.callback.finish();
        }
    }

    private BufferedReader getGzipContent(ObsObject obsObject) {
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
            return new BufferedReader(inputStreamReader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (gzIn != null) {
                    gzIn.close();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private BufferedReader getZipContent(ObsObject obsObject) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ZipArchiveInputStream zipArchiveInputStream = new ZipArchiveInputStream(new ZipInputStream(obsObject.getObjectContent()));
        try {
            ZipArchiveEntry tarEntry;
            while ((tarEntry = zipArchiveInputStream.getNextZipEntry()) != null) {
                if (tarEntry.isDirectory()) {
                    continue;
                }
                byte[] buffer = new byte[1024];
                int n;
                while ((n = zipArchiveInputStream.read(buffer, 0, 1024)) >= 0) {
                    bos.write(buffer, 0, n);
                }

                // 将CSV数据解析为Java对象
                ByteArrayInputStream fileInputStream = new ByteArrayInputStream(bos.toByteArray());
                InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream);
                return new BufferedReader(inputStreamReader);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                zipArchiveInputStream.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    public interface FinishedCallback {
        void finish();
    }

}
