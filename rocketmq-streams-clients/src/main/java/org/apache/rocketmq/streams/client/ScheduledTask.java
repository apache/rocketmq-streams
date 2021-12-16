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

import java.util.Date;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.connectors.source.filter.CycleSchedule;
import org.apache.rocketmq.streams.db.sink.EnhanceDBSink;

/**
 * @description
 */
public class ScheduledTask implements Runnable{

    CycleSchedule schedule;
    String sinkTableName;
    String sourceTableName;
    String url;
    String userName;
    String password;

    public ScheduledTask(String expression, String url, String userName, String password, String sourceTableName, String sinkTableName){
        schedule = CycleSchedule.getInstance(expression, new Date());
        this.sourceTableName = sourceTableName;
        this.sinkTableName = sinkTableName;
        this.url = url;
        this.userName = userName;
        this.password = password;
        ComponentCreator.getProperties().put(ConfigureFileKey.CHECKPOINT_STORAGE_NAME, "db");
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, url);//数据库连接url
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, userName);//用户名
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, password);//password
        ComponentCreator.getProperties().put(ConfigureFileKey.IS_ATOMIC_DB_SINK, "true");

    }


    @Override
    public void run() {
        CycleSchedule.Cycle cycle = schedule.nextCycle(new Date());
        DataStreamSource dataStreamsource = StreamBuilder.dataStream("test_baseline" + "_" + cycle.getCycleDateStr(), "baseline_pipeline");
        DataStream datastream = dataStreamsource.fromCycleSource(url, userName, password, sourceTableName, cycle, 3);
        EnhanceDBSink sink = new EnhanceDBSink();
        sink.setAtomic(true);
        sink.setTableName(sinkTableName);
        sink.setUrl(url);
        sink.setUserName(userName);
        sink.setPassword(password);
        sink.init();
        datastream.to(sink).start(true);
    }
}
