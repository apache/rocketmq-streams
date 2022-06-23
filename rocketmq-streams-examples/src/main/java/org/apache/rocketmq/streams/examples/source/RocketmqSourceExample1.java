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
package org.apache.rocketmq.streams.examples.source;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.examples.aggregate.ProducerFromFile;

import static org.apache.rocketmq.streams.examples.aggregate.Constant.NAMESRV_ADDRESS;


public class RocketmqSourceExample1 {
    private static String topicName = "topic-1";
    private static String groupName = "groupName-1";

    public static void main(String[] args) {
        ProducerFromFile.produce("data.txt",NAMESRV_ADDRESS, topicName);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }

        System.out.println("begin streams code.");

        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        /**
         * 1、before run this case, make sure some data has already been rocketmq.
         */
        source.fromRocketmq(
                topicName,
                groupName,
                NAMESRV_ADDRESS
        )
                .map(message -> message)
                .toPrint(1)
                .start();

    }
}
