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
package org.apache.rocketmq.streams.dim.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.compress.AbstractMemoryTable;
import org.apache.rocketmq.streams.common.utils.FileUtil;

public class FileDim extends AbstractDim {
    protected String filePath;

    @Override
    protected void loadData2Memory(AbstractMemoryTable tableCompress) {
        List<String> rows = FileUtil.loadFileLine(filePath);
        for (String row : rows) {
            JSONObject jsonObject = JSON.parseObject(row);
            Map<String, Object> values = new HashMap<>();
            for (String key : jsonObject.keySet()) {
                values.put(key, jsonObject.getString(key));
            }
            tableCompress.addRow(values);
        }
    }

    public static void main(String[] args) {
        List<String> lines = FileUtil.loadFileLine("/tmp/data_model_extractor_config.txt");
        for (String row : lines) {
            JSONObject jsonObject = JSON.parseObject(row);
            System.out.println(jsonObject);
        }
    }

    @Override
    public String getFilePath() {
        return filePath;
    }

    @Override
    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
