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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.SplitStream;
import org.apache.rocketmq.streams.common.configuration.JobConfiguration;
import org.apache.rocketmq.streams.common.functions.SplitFunction;
import org.junit.Test;

public class MqttSourceExample implements Serializable {

    public static void main(String[] args) throws InterruptedException {
        DataStreamSource dataStream = StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name", new JobConfiguration());
        SplitStream ds = StreamExecutionEnvironment.getExecutionEnvironment().create("tmp", "tmp")
            .fromFile("window_msg_10.txt", true)
            .split(new SplitFunction<JSONObject>() {
                @Override public String split(JSONObject o) {
                    return o.getString("ProjectName");
                }
            });

        ds.select("project-2").toPrint();
        ds.select("project-3").toPrint();
        ds.select("project-7").toPrint();
        ds.select("project-1").toPrint().start();
    }

    @Test public void test1() {
        DataStreamSource dataStreamSource = StreamExecutionEnvironment.getExecutionEnvironment().create("", "");
        DataStream ds = dataStreamSource.fromMqtt("tcp://host:port", "", "", "", "")
            .flatMap(message -> {
                JSONObject obj = ((JSONObject) message);
                JSONArray array = obj.getJSONArray("Data");
                String topic = obj.getString("__topic");
                for (int i = 0; i < array.size(); i++) {
                    array.getJSONObject(i).put("__topic", topic);
                }
                return array;
            });
//        ds.toPrint().start();

        SplitStream splitStream = ds.split(new SplitFunction<JSONObject>() {
            @Override public String split(JSONObject o) {
                return o.getString("AttributeCode");
            }
        });
        splitStream.select("Read").toPrint();
        splitStream.select("Write").toPrint().start();
        //splitStream.select("Write").toPrint().with(ShuffleStrategy.shuffleWithMemory());
    }

}
