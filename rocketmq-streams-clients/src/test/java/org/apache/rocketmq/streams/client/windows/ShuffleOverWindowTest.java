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
package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.junit.Test;

public class ShuffleOverWindowTest {
    protected String filePath = "/tmp/over.txt";

    @Test
    public void testShuffleWindow() {
        StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name1")
            .fromFile(filePath, true)
            .topN("rowNum", 10000, "city")
            .addOrderByFieldName("name", true)
            .addOrderByFieldName("age", false)
            .toDataSteam()
            .toPrint()
            .start();
    }

    @Test
    public void testCreateData() {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            JSONObject msg = new JSONObject();
            msg.put("city", "beijing");
            msg.put("name", "chris" + i % 10);
            msg.put("age", i);
            list.add(msg.toJSONString());
            System.out.println(msg);
        }
        FileUtil.write(filePath, list);
    }
}
