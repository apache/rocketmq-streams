/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.streams.examples.checkpoint;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.dbinit.mysql.delegate.DBDelegate;
import org.apache.rocketmq.streams.dbinit.mysql.delegate.DBDelegateFactory;
import org.apache.rocketmq.streams.examples.aggregate.ProducerFromFile;

import static org.apache.rocketmq.streams.examples.aggregate.Constant.NAMESRV_ADDRESS;


public class RemoteCheckpointExample {
    //replace with your mysql url, database name can be anyone else.
    private static final String URL = "jdbc:mysql://localhost:3306/rocketmq_streams";
    // user name of mysql
    private static final String USER_NAME = "";
    //password of mysql
    private static final String PASSWORD = "";

    private static final String TOPIC_NAME = "mysql_checkpoint_topic";

    private static final String GROUP_NAME = "mysql_checkpoint_group_id";

    static  {
        ComponentCreator.getProperties().put(ConfigureFileKey.CONNECT_TYPE, "DB");
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_URL, URL);
        //if you mysql version large than 8.0, use com.mysql.cj.jdbc.Driver as a driver.
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_DRIVER, "com.mysql.cj.jdbc.Driver");
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_USERNAME, USER_NAME);
        ComponentCreator.getProperties().put(ConfigureFileKey.JDBC_PASSWORD, PASSWORD);
    }

    public static void main(String[] args) {
        ProducerFromFile.produce("data.txt",NAMESRV_ADDRESS, TOPIC_NAME);
        DBDelegate delegate = DBDelegateFactory.getDelegate();
        delegate.init();

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }
        System.out.println("begin streams code.");

        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromRocketmq(
                TOPIC_NAME,
                GROUP_NAME,
                false,
                NAMESRV_ADDRESS)
                .filter((message) -> {
                    try {
                        JSONObject.parseObject((String) message);
                    } catch (Throwable t) {
                        // if can not convert to json, discard it.because all operator are base on json.
                        return false;
                    }
                    return true;
                })
                //must convert message to json.
                .map(message -> JSONObject.parseObject((String) message))
                .window(TumblingWindow.of(Time.seconds(10)))
                .groupBy("ProjectName","LogStore")
                .sum("OutFlow", "OutFlow")
                .sum("InFlow", "InFlow")
                .count("total")
                .waterMark(5)
                .setLocalStorageOnly(false)
                .toDataStream()
                .toPrint(1)
                .start();
    }

}
