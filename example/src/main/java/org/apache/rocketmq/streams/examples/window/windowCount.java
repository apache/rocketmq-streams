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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.runtime.operators.Time;
import org.apache.rocketmq.streams.core.runtime.operators.TimeType;
import org.apache.rocketmq.streams.core.runtime.operators.WindowBuilder;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class windowCount {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder("windowCount");
        builder.source("windowCount", source -> {
                    String value = new String(source, StandardCharsets.UTF_8);
                    int result = Integer.parseInt(value);
                    return new Pair<>(null, result);
                })
                .filter(value -> value > 0)
                .keyBy(value -> "key")
                .window(WindowBuilder.tumblingWindow(Time.seconds(15)))
                .count()
                .toRStream()
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
