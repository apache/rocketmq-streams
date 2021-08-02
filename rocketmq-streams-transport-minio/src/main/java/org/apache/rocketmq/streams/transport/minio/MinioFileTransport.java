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
package org.apache.rocketmq.streams.transport.minio;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.model.ServiceName;
import org.apache.rocketmq.streams.common.transport.AbstractFileTransport;
import org.apache.rocketmq.streams.common.transport.IFileTransport;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import com.google.auto.service.AutoService;
import io.minio.MinioClient;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

@AutoService(IFileTransport.class)
@ServiceName(MinioFileTransport.NAME)
public class MinioFileTransport extends AbstractFileTransport {
    public static final String NAME = "minio";
    protected String ak;
    protected String sk;
    protected String endpoint;
    protected String dirpperDir;
    protected MinioClient minioClient;

    public MinioFileTransport() {
        this.ak = ComponentCreator.getProperties().getProperty(ConfigureFileKey.FILE_TRANSPORT_AK);
        this.sk = ComponentCreator.getProperties().getProperty(ConfigureFileKey.FILE_TRANSPORT_SK);
        this.endpoint = ComponentCreator.getProperties().getProperty(ConfigureFileKey.FILE_TRANSPORT_ENDPOINT);
        this.dirpperDir = ComponentCreator.getProperties().getProperty(ConfigureFileKey.FILE_TRANSPORT_DIPPER_DIR);
        if (StringUtil.isEmpty(this.dirpperDir)) {
            this.dirpperDir = "dipper_files";
        }
    }

    @Override
    public File download(String remoteFileName, String localDir, String localFileName) {
        MinioClient minioClient = getOrCreateMinioClient();
        BufferedWriter bw = null;
        BufferedReader br = null;
        try {
            InputStream input = minioClient.getObject(dirpperDir, remoteFileName);
            File file = new File(FileUtil.concatFilePath(localDir, localFileName));
            bw = new BufferedWriter(new FileWriter(file));
            br = new BufferedReader(new InputStreamReader(input));
            String line = br.readLine();
            while (line != null) {
                bw.write(line);
                line = br.readLine();
            }
            bw.flush();
            return file;
        } catch (Exception e) {
            throw new RuntimeException("dowload file error " + dirpperDir + "/" + remoteFileName, e);

        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public Boolean upload(File file, String remoteFileName) {
        MinioClient minioClient = getOrCreateMinioClient();
        try {
            minioClient.putObject(dirpperDir, remoteFileName, file.getAbsolutePath());
        } catch (Exception e) {
            throw new RuntimeException("upload file error " + dirpperDir + "/" + remoteFileName, e);
        }
        return true;
    }

    @Override
    public boolean delete(String remoteFileName) {
        MinioClient minioClient = getOrCreateMinioClient();
        try {
            minioClient.removeObject(dirpperDir, remoteFileName);
        } catch (Exception e) {
            throw new RuntimeException("delete file error " + dirpperDir + "/" + remoteFileName, e);
        }
        return true;
    }

    protected MinioClient getOrCreateMinioClient() {
        if (this.minioClient == null) {
            synchronized (this) {
                if (minioClient == null) {
                    try {
                        MinioClient minioClient = new MinioClient(endpoint, ak, sk);
                        boolean existDir = minioClient.bucketExists(dirpperDir);

                        if (!existDir) {
                            minioClient.makeBucket(dirpperDir);
                        }
                        this.minioClient = minioClient;
                    } catch (Exception e) {
                        throw new RuntimeException("create minio client error", e);
                    }

                }
            }
        }
        return this.minioClient;
    }
}


