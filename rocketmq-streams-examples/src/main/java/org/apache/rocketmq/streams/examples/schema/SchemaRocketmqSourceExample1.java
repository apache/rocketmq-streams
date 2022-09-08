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
package org.apache.rocketmq.streams.examples.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.examples.send.ProducerFromFile;
import org.apache.rocketmq.streams.examples.source.Data;
import org.apache.rocketmq.streams.schema.SchemaConfig;
import org.apache.rocketmq.streams.schema.SchemaType;

import static org.apache.rocketmq.streams.examples.aggregate.Constant.NAMESRV_ADDRESS;

public class SchemaRocketmqSourceExample1 {
    private static String topicName = "schema-topic-1";
    private static String groupName = "groupName-1";

    public static void main(String[] args) {

        ProducerFromFile.produce("data-2.txt", NAMESRV_ADDRESS, topicName, true);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }

        System.out.println("begin streams code.");

        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromRocketmq(
            topicName,
            groupName,
            NAMESRV_ADDRESS,
            new SchemaConfig(SchemaType.JSON, Data.class))
            .forEach(data ->
                System.out.println("test forEach: " + data.toString()))
            .filter(data -> ((Data)data).getInFlow() > 5)
            .flatMap((data) -> {
                List<String> result = new ArrayList<>();
                result.add("test flatMap Input = " + ((Data)data).getInFlow());
                result.add("test flatMap Output = " + ((Data)data).getOutFlow());
                return result;
            })
            .toPrint(1)
            .start();

    }

}
