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
package org.apache.rocketmq.streams.client.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.client.transform.SplitStream;
import org.apache.rocketmq.streams.common.functions.SplitFunction;

public class SplitTest {

    public static void main(String[] args) throws InterruptedException {
        SplitStream ds = StreamExecutionEnvironment.getExecutionEnvironment().create("tmp", "tmp")
            .fromFile("window_msg_10.txt", true)
            .split(new SplitFunction<JSONObject>() {
                @Override public String split(JSONObject o) {
                    return o.getString("ProjectName");
                }
            });

        ds.select("project-2").toPrint();
        ds.select("project-7").toPrint();
        ds.select("project-1").toPrint();

        ds.toDataStream().start();

    }

}
