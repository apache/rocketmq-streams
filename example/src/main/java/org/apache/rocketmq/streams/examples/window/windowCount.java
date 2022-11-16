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
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.rstream.WindowStream;
import org.apache.rocketmq.streams.core.runtime.operators.Time;
import org.apache.rocketmq.streams.core.runtime.operators.TimeType;
import org.apache.rocketmq.streams.core.runtime.operators.WindowBuilder;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.examples.pojo.Num;
import org.apache.rocketmq.streams.examples.pojo.User;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * 1、启动RocketMQ
 * 2、创建topic
 * 3、启动本例子运行
 * 4、向topic中写入数据
 * 5、观察输出结果
 */
public class windowCount {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("windowCountUser");

        AggregateAction<String,User, Num> aggregateAction = (key, value, accumulator) -> new Num(value.getName(), 100);

        WindowStream<String, Num> user = builder.source("user", source -> {
                    User user1 = JSON.parseObject(source, User.class);
                    return new Pair<>(null, user1);
                })
                .filter(value -> value.getAge() > 0)
                .keyBy(value -> "key")
                .window(WindowBuilder.tumblingWindow(Time.seconds(15)))
                .aggregate(aggregateAction);

        user.toRStream()
                .print();

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.putIfAbsent(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        properties.put(Constant.TIME_TYPE, TimeType.EVENT_TIME);
        properties.put(Constant.ALLOW_LATENESS_MILLISECOND, 2000);

        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        rocketMQStream.start();
    }


}
