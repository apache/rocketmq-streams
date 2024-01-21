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
package org.apache.rocketmq.streams.examples.join;

import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.client.transform.DataStream;

public class RocketmqJoinExample {
    public static void main(String[] args) {
        DataStream left = StreamExecutionEnvironment.getExecutionEnvironment().create("tmp", "tmp")
            .fromRocketmq("TopicTest", "groupA", true, "localhost:9876");
        DataStream right = StreamExecutionEnvironment.getExecutionEnvironment().create("tmp", "tmp")
            .fromRocketmq("TopicTest", "groupB", true, "localhost:9876");

        left.join(right)
            .on("(ProjectName,=,ProjectName)")
            .toDataSteam()
            .toPrint()
            .start();
    }

}
