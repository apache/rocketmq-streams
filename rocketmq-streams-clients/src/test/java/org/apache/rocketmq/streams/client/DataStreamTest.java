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
import java.io.Serializable;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.junit.Before;
import org.junit.Test;

public class DataStreamTest implements Serializable {

    DataStreamSource dataStream;

    @Before
    public void init() {
        dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");
    }

    @Test
    public void testFromFile() {
        dataStream
            .fromFile("/Users/junjie.cheng/test.sql", false)
            .map(message -> message + "--")
            .toPrint(1)
            .start();
    }

    @Test
    public void testRocketmq() {
        DataStreamSource dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");
        dataStream
            .fromRocketmq("topic_xxxx01", "consumer_xxxx01", "127.0.0.1:9876")
            .map(message -> message + "--")
            .toPrint(1)
            .start();
    }

    @Test
    public void testDBCheckPoint() {
        dataStream
            .fromRocketmq("topic_xxxx02", "consumer_xxxx02", "服务器.:9876")
            .map(message -> message + "--")
            .toPrint(1)
            .with(WindowStrategy.exactlyOnce("", "", ""))
            .start();
    }

    @Test
    public void testFileCheckPoint() {
        dataStream
            .fromFile("/Users/junjie.cheng/text.txt", false)
            .map(message -> message + "--")
            .toPrint(1)
            .with(WindowStrategy.highPerformance())
            .start();
    }

    @Test
    public void testWindow() {
        DataStreamSource dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");
        dataStream
            .fromRocketmq("topic_xxxx03", "consumer_xxxx03", "127.0.0.1:9876")
            .map(new MapFunction<JSONObject, String>() {

                @Override
                public JSONObject map(String message) throws Exception {
                    JSONObject msg = JSONObject.parseObject(message);
                    return msg;
                }
            })
            .window(TumblingWindow.of(Time.seconds(5)))
            .groupBy("name", "age")
            .count("c")
            .sum("score", "scoreValue")
            .toDataSteam()
            .toPrint(1)
            .with(WindowStrategy.exactlyOnce("", "", ""))
            .start();
    }

    @Test
    public void testFingerPrintStrategy() {
        dataStream
            .fromFile("/Users/junjie.cheng/text.txt", false)
            .map(message -> message + "--")
            .toPrint(1)
            .start(true);

    }

    @Test
    public void testBothStrategy() {
        dataStream
            .fromRocketmq("topic_xxxx04", "consumer_xxxx04", "127.0.0.1:9876")
            .map(message -> message + "--")
            .filter(message -> {
                return true;
            })
            .toPrint(1)
            .with()
            .start();
    }

    @Test
    public void testMeta() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection connection = DriverManager.getConnection("", "", "");
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet dataFilter = metaData.getColumns(connection.getCatalog(), "%", "XXX", null);
            while (dataFilter.next()) {
                String one = dataFilter.getString("DATA_TYPE");
                int two = dataFilter.getInt("COLUMN_SIZE");
                String three = dataFilter.getString("COLUMN_NAME");
                String four = dataFilter.getString("TYPE_NAME");
                System.out.println(one + "    " + two + "     " + three + "   " + four + "    " + DataTypeUtil.getDataType(dataFilter.getString("TYPE_NAME")));
            }

        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

}
