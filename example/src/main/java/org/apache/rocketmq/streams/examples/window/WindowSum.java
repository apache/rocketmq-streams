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
package org.apache.rocketmq.streams.examples.window;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.window.Time;
import org.apache.rocketmq.streams.core.window.WindowBuilder;
import org.apache.rocketmq.streams.examples.pojo.Grade;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

/**
 * 1、启动RocketMQ
 * 2、创建topic
 * 3、启动本例子运行
 * 4、向topic中写入数据
 * 5、观察输出结果
 */
public class WindowSum {
    private static final SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Throwable {
        StreamBuilder builder = new StreamBuilder("windowSumGrade");

        builder.source("grade", source -> {
                    Grade grade = JSON.parseObject(source, Grade.class);
                    System.out.println("time=" + format.format(new Date(System.currentTimeMillis())) + "," + grade);
                    return new Pair<>(null, grade);
                })
                .keyBy(Grade::getGrade)
                .window(WindowBuilder.tumblingWindow(Time.seconds(5)))
                .sum(Grade::getScore)
                .toRStream()
                .print();

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.putIfAbsent(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        properties.put(StreamConfig.ALLOW_LATENESS_MILLISECOND, 2000);

        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        Runtime.getRuntime().addShutdownHook(new Thread("wordcount-shutdown-hook") {
            @Override
            public void run() {
                rocketMQStream.stop();
            }
        });

        rocketMQStream.start();
    }
}

