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
package org.apache.rocketmq.streams.client;

import java.util.Properties;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.ShuffleStrategy;
import org.apache.rocketmq.streams.client.strategy.Strategy;
import org.junit.Test;

public class MQTTTest {
    @Test
    public void testMQTT() {

        DataStreamSource dataStream = StreamExecutionEnvironment.getExecutionEnvironment().create("test_namespace", "graph_pipeline");
        dataStream.fromFile("window_msg_10.txt", true)
            .script("x=py('filename','字段1',ProjectName,LogStore)")
            .with(ShuffleStrategy.shuffleWithMemory())
            .with(new Strategy() {
                @Override public Properties getStrategyProperties() {
                    Properties properties = new Properties();
                    properties.put("dipper.udf.jar.path", "python目录");
                    return properties;
                }
            })
            .toPrint()
            .start();
    }
}
