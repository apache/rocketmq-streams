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

package org.apache.rocketmq.streams.examples.aggregate;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.examples.send.ProducerFromFile;

public class UsersDimensionExample {
    private static final String topic = "pageClick";
    private static final String namesrv = "127.0.0.1:9876";

    /**
     * Count the number of times a user clicks on a webpage within 5s
     * @param args
     */
    public static void main(String[] args) {
        ProducerFromFile.produce("pageClickData.txt",namesrv, topic, true);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }
        System.out.println("begin streams code.");


        DataStreamSource source = StreamBuilder.dataStream("pageClickNS", "pageClickPL");
        source.fromRocketmq(topic, "pageClickGroup", false, namesrv)
                .map(message -> JSONObject.parseObject((String) message))
                .window(TumblingWindow.of(Time.minutes(1)))
                .groupBy("userId")
                .setTimeField("eventTime")
                .count("total")
                .waterMark(1)
                .setLocalStorageOnly(true)
                .toDataStream()
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();

    }



}
