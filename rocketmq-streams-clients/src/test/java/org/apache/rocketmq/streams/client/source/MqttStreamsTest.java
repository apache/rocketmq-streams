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
package org.apache.rocketmq.streams.client.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.strategy.ShuffleStrategy;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.junit.Before;
import org.junit.Test;

public class MqttStreamsTest {
    DataStreamSource dataStream;

    @Before
    public void init() {
        dataStream = StreamBuilder.dataStream("test_namespace", "graph_pipeline");
    }

    @Test
    public void testMqtt() {
        dataStream.fromMqtt("", "", "", "", "")
            .flatMap(message -> ((JSONObject) message).getJSONArray("Data"))
            .window(TumblingWindow.of(Time.minutes(1)))
            .groupBy("AttributeCode")
            .setLocalStorageOnly(true)
            .avg("Value", "avg_value")
            .toDataSteam()
            .toPrint()
            .with(ShuffleStrategy.shuffleWithMemory())
            .start();
    }

}
