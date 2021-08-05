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
package org.apache.rocketmq.streams.db.sink.db;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.db.sink.DBSink;
import org.junit.Test;

public class DBWriteOnlyChannelTest {

    private String URL = "jdbc:mysql://XXXXX:3306/yundun_soc?useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    protected String USER_NAME = "XXXX";
    protected String PASSWORD = "XXXX";

    @Test
    public void testOutputBySQL() {
        String sql = "insert into table(name,age) values('#{name}',#{age})";
        DBSink sink = new DBSink(sql, URL, USER_NAME, PASSWORD) {

            /**
             * 因为不是真实表，会报错，把执行sql，改成打印sql
             */
            @Override
            protected void executeSQL(JDBCDriver dbDataSource, String sql) {
                System.out.println(sql);
            }
        };
        for (int i = 0; i < 10; i++) {
            JSONObject msg = new JSONObject();
            msg.put("name", "chris" + i);
            msg.put("age", i);
            IMessage message = new Message(msg);
            sink.batchAdd(message);
        }
        sink.flush();
    }

    @Test
    public void testOutputByMetaData() {
        DBSink sink = new DBSink() {
            /**
             * 因为不是真实表，会报错，把执行sql，改成打印sql
             */
            @Override
            protected void executeSQL(JDBCDriver dbDataSource, String sql) {
                System.out.println(sql);
            }
        };
        JSONObject msg = new JSONObject();
        msg.put("name", "chris");
        msg.put("age", 18);
        MetaData metaData = MetaData.createMetaData(msg);
        metaData.setTableName("tableName");
        sink.setMetaData(metaData);
        for (int i = 0; i < 10; i++) {
            msg = new JSONObject();
            msg.put("name", "chris" + i);
            msg.put("age", i);
            IMessage message = new Message(msg);
            sink.batchAdd(message);
        }
        sink.flush();
    }

}
