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

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;

import java.util.Arrays;

public class RocketMQSourceExample3 {
    public static final String NAMESRV_ADDRESS = "127.0.0.1:9876";
    public static final String RMQ_TOPIC = "NormalTestTopic";
    public static final String RMQ_CONSUMER_GROUP_NAME = "test-group-03";
    public static final String TAGS = "*";

    /**
     * 1、before run this case, make sure some data has already been rocketmq.
     */
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");

        source.fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                false,
                NAMESRV_ADDRESS)
                .forEach((message) -> {
                    System.out.println("forEach: " + message);
                })
                .map(message -> message)
                .filter((value) -> {
                    String messageValue = (String) value;
                    return messageValue.contains("RocketMQ");
                })
                .flatMap((message) -> {
                    String value = (String) message;
                    String[] result = value.split(" ");
                    return Arrays.asList(result);
                })
                .toPrint(1)
                .start();

    }


}
