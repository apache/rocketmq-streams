package org.apache.rocketmq.streams.examples.sink;
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.core.RocketMQStream;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.rstream.StreamBuilder;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * 1、启动RocketMQ
 * 2、创建topic sh bin/mqadmin updateTopic -c DefaultCluster -t sourceTopic -r 8 -w 8 -n 127.0.0.1:9876
 * 3、启动本例子运行
 * 4、向topic中写入数据
 * 5、观察输出结果
 */
public class WordCountSink {
    public static void main(String[] args) throws Throwable {
        StreamBuilder builder = new StreamBuilder("wordCount");

        builder.source("sourceTopic", total -> {
                    String value = new String(total, StandardCharsets.UTF_8);
                    return new Pair<>(null, value);
                })
                .flatMap((ValueMapperAction<String, List<String>>) value -> {
                    String[] splits = value.toLowerCase().split("\\W+");
                    return Arrays.asList(splits);
                })
                .keyBy(value -> value)
                .count()
                .sink("wordCountSink", new KeyValueSerializer<String, Integer>() {
                    final ObjectMapper objectMapper = new ObjectMapper();

                    @Override
                    public byte[] serialize(String o, Integer data) throws Throwable {
                        ObjectNode objectNode = objectMapper.createObjectNode();
                        objectNode.put(o, data);

                        String result = objectNode.toPrettyString();
                        return objectMapper.writeValueAsBytes(result);
                    }
                });

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.put(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");

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
