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

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.junit.Test;

/**
 * /**
 *
 * @description
 */
public class SourceMergerTest {

    @Test
    public void testUnion() {
        // ComponentCreator.getProperties().put(ConfigureFileKey.CONNECT_TYPE, IConfigurableService.JOB_SERVICE_NAME);
        DataStream formatStream = (StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name")
            .fromFile("window_msg_100.txt")
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("left", "1");
                    return message;
                }
            }));

        DataStream wendu = formatStream
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("wendu", true);
                    return message;
                }
            });

        DataStream shidu = formatStream
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("shidu", true);
                    return message;
                }
            });

        DataStream liangdu = formatStream
            .map(new MapFunction<JSONObject, JSONObject>() {
                @Override public JSONObject map(JSONObject message) throws Exception {
                    message.put("liangdu", true);
                    return message;
                }
            });

        wendu.union(shidu).union(liangdu).toPrint().start();

    }

}
