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

package org.apache.rocketmq.streams.examples.rocketmqsource;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.JoinStream;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.examples.mutilconsumer.Producer;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.*;

public class MultiRocketMQSourceStreamsExample {
    private static ExecutorService producerPool = Executors.newFixedThreadPool(2);
    private static ExecutorService consumerPool = Executors.newCachedThreadPool();
    private static Random random = new Random();


    public static void main(String[] args) {
        //producer
        producerPool.submit(new Runnable() {
            @Override
            public void run() {
                Producer.produceInLoop(RMQ_TOPIC, "data.txt");
            }
        });

        //producer
        producerPool.submit(new Runnable() {
            @Override
            public void run() {
                Producer.produceInLoop(RMQ_TOPIC_OTHER, "data.txt");
            }
        });


        //consumer
        for (int i = 0; i < 1; i++) {
            consumerPool.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        runOneStreamsClient(77);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }

    }

    private static void runOneStreamsClient(int index) {
        int namespaceIndex = index;
        int pipelineIndex = index;
        DataStreamSource leftSource = StreamBuilder.dataStream("namespace" + namespaceIndex, "pipeline" + pipelineIndex);
        DataStream left = leftSource.fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                true,
                NAMESRV_ADDRESS);

        int otherPipelineIndex = index + 1;
        DataStreamSource rightSource = StreamBuilder.dataStream("namespace" + namespaceIndex, "pipeline" + otherPipelineIndex);
        DataStream right = rightSource.fromRocketmq(
                RMQ_TOPIC_OTHER,
                RMQ_CONSUMER_GROUP_NAME_OTHER,
                true,
                NAMESRV_ADDRESS);

        left.join(right)
                .setJoinType(JoinStream.JoinType.LEFT_JOIN)
                .setCondition("(InFlow,==,InFlow)")
                .window(Time.minutes(1))
                .toDataSteam()
                .map(message -> {
                    System.out.println(message);
                    return message + "===";
                })
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
