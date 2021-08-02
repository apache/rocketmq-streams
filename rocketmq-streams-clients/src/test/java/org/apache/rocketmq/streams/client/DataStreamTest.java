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

import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.CheckpointStrategy;
import org.apache.rocketmq.streams.client.strategy.StateStrategy;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.sql.*;

public class DataStreamTest implements Serializable {

    DataStreamSource dataStream;

    @Before
    public void init() {
        dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");
    }

    @Test
    public void testFromFile() {
        dataStream
            .fromFile("/Users/junjie.cheng/text.txt", false)
            .map(message -> message + "--")
            .toPrint(1)
            .start();
    }

    @Test
    public void testRocketmq() {
        dataStream
            .fromRocketmq("TOPIC_EVENT_SAS_SECURITY_EVENT", "111")
            .map(message -> message + "--")
            .toPrint(1)
            .start();
    }

    @Test
    public void testDBCheckPoint() {
        dataStream
            .fromRocketmq("TSG_META_INFO", "")
            .map(message -> message + "--")
            .toPrint(1)
            .with(CheckpointStrategy.db("", "", "", 0L))
            .start();
    }

    @Test
    public void testFileCheckPoint() {
        dataStream
            .fromRocketmq("TSG_META_INFO", "")
            .map(message -> message + "--")
            .toPrint(1)
            .with(CheckpointStrategy.mem(0L))
            .start();
    }

    @Test
    public void testBothStrategy() {
        dataStream
            .fromRocketmq("TSG_META_INFO", "")
            .map(message -> message + "--")
            .toPrint(1)
            .with(CheckpointStrategy.mem(0L), StateStrategy.db())
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
