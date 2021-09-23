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

package org.apache.rocketmq.streams.examples.rocketmqsource;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;

import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.NAMESRV_ADDRESS;
import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.RMQ_CONSUMER_GROUP_NAME;
import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.RMQ_TOPIC;

public class RocketmqWindowTest {

    /**
     * 1、before run this case, make sure some data has already been rocketmq.
     * 2、rocketmq allow create topic automatically.
     */
    public static void main(String[] args) {
        ProducerFromFile.produce("data.txt",NAMESRV_ADDRESS, RMQ_TOPIC);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }
        System.out.println("begin streams code.");

        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                false,
                NAMESRV_ADDRESS)
                .filter((message) -> {
                    try {
                        JSONObject.parseObject((String) message);
                    } catch (Throwable t) {
                        // if can not convert to json, discard it.because all operator are base on json.
                        return true;
                    }
                    return false;
                })
                //must convert message to json.
                .map(message -> JSONObject.parseObject((String) message))
                .window(TumblingWindow.of(Time.seconds(10)))
                .groupBy("ProjectName","LogStore")
                .sum("OutFlow", "OutFlow")
                .sum("InFlow", "InFlow")
                .count("total")
                .waterMark(5)
                .setLocalStorageOnly(true)
                .toDataSteam()
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();

    }

}
