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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.junit.Before;
import org.junit.Test;

/**
 * /**
 *
 * @description
 */
public class DBSinkTest {

    DataStreamSource dataStream;

    String url = "*";
    String userName = "*";
    String password = "*";
    String tableName = "*";

    @Before
    public void init() {
        dataStream = StreamExecutionEnvironment.getExecutionEnvironment().create("test_namespace", "graph_pipeline");
        // ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
    }

    @Test
    public void testToMultiDB() {
        List<JSONObject> list = new ArrayList();
        String[] partitions = new String[] {
            "20210709000000",
            "20210710000000"
        };
        for (int i = 0; i < 100000; i++) {
            //0,1随机数，整数
            int index = (int) (2 * Math.random());
            JSONObject msg = new JSONObject();
            msg.put("ds", partitions[index]);
            msg.put("value", String.valueOf(Math.random()));
            msg.put("data_time", new Date());
            list.add(msg);
        }
        System.setProperty("log4j.home", System.getProperty("user.home") + "/logs");
        System.setProperty("log4j.level", "DEBUG");
        dataStream.fromArray(list.toArray()).toMultiDB(url, userName, password, tableName, "ds").start();
    }

    @Test
    public void testToEnhanceDB() {
        List<JSONObject> list = new ArrayList();
        for (int i = 0; i < 100000; i++) {
            //0,1随机数，整数
            int index = (int) (2 * Math.random());
            JSONObject msg = new JSONObject();
            msg.put("name", String.valueOf(Math.random()));
            msg.put("age", new Random().nextInt());
            msg.put("date_time", new Date());
            list.add(msg);
        }
        dataStream.fromArray(list.toArray()).toEnhanceDBSink(url, userName, password, "mock_table_name").start();
    }

}
