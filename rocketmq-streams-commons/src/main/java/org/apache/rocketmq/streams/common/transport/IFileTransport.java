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
package org.apache.rocketmq.streams.common.transport;

import java.io.File;

public interface IFileTransport {

    /**
     * 下载文件，把文件放到loacal目录下，文件名=localFileName
     *
     * @param remoteFileName 在远程存储上的文件名
     * @param localDir       下载到本地的当前目录中
     * @param localFileName  下载到本地的当前目录中，文件重命名为 localFileName
     * @return 返回下载的文件
     */
    File download(String remoteFileName, String localDir, String localFileName);

    /**
     * 下载文件，把文件放到loacal目录下，文件名=remoteFileName
     *
     * @param remoteFileName
     * @param localDir
     * @return
     */
    File download(String remoteFileName, String localDir);

    File download(String remoteFileName);

    /**
     * 上传本地文件，到远程目录，文件重新命名为remoteFileName
     *
     * @param file           待上传的文件
     * @param remoteFileName 需要重新命名的文件
     * @return
     */
    Boolean upload(File file, String remoteFileName);

    /**
     * 上传本地文件，到远程目录，文件名=file.name
     *
     * @param file 待上传的文件
     * @return
     */
    Boolean upload(File file);

    /**
     * 删除远程文件
     *
     * @param remoteFileName
     * @return
     */
    boolean delete(String remoteFileName);
}
