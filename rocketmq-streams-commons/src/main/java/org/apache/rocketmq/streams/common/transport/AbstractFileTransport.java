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
import org.apache.rocketmq.streams.common.utils.FileUtil;

public abstract class AbstractFileTransport implements IFileTransport {

    @Override
    public File download(String remoteFileName, String localDir) {
        return download(remoteFileName, localDir, remoteFileName);
    }

    @Override
    public File download(String remoteFileName) {
        File tmpDir = FileUtil.createTmpFile("tmp_file_down");
        return download(remoteFileName, tmpDir.getAbsolutePath(), remoteFileName);
    }

    @Override
    public Boolean upload(File file) {
        return upload(file, file.getName());
    }

}
