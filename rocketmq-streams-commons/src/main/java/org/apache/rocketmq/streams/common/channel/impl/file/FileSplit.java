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
package org.apache.rocketmq.streams.common.channel.impl.file;

import com.alibaba.fastjson.JSONObject;
import java.io.File;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;

public class FileSplit extends BasedConfigurable implements ISplit<FileSplit, File> {
    protected transient File file;
    private String filePath;
    protected String splitId;

    @Override
    public String getQueueId() {
        return filePath + "_" + splitId;
    }

    @Override
    public File getQueue() {
        return file;
    }

    @Override
    public int compareTo(FileSplit o) {
        return splitId.compareTo( o.splitId);
    }

    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        super.getJsonObject(jsonObject);
        File file = new File(filePath);
        this.file = file;
    }

    public FileSplit(File file) {
        this.filePath = file.getAbsolutePath();
        splitId = this.filePath;
        this.file = file;
    }
}
