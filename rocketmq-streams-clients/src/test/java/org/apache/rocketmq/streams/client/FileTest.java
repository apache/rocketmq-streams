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
package org.apache.rocketmq.streams.client;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.junit.Test;

public class FileTest {
    @Test
    public void testFilter() {
        DataStreamSource dataStream = DataStreamSource.create("namespace", "name");
        dataStream.fromFile("/tmp/file.txt", false)
            .filter((message) -> {
                JSONObject jsonObject = JSON.parseObject((String) message);
                if (Objects.nonNull(jsonObject)) {
                    int inFlow = jsonObject.getIntValue("InFlow");
                    if (inFlow > 2) {
                        return false;
                    } else {
                        return true;
                    }
                }
                return true;

            }).toPrint(1)
            .start();

    }

    @Test
    public void testWriteFile() {
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("InFlow", i);
            lines.add(jsonObject.toJSONString());
        }
        lines.add("");
        FileUtil.write("/tmp/file.txt", lines);
    }
}
