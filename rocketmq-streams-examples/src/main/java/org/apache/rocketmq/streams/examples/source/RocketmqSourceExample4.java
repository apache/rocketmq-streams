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

package org.apache.rocketmq.streams.examples.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.examples.aggregate.ProducerFromFile;

import static org.apache.rocketmq.streams.examples.aggregate.Constant.NAMESRV_ADDRESS;

public class RocketmqSourceExample4 {
    private static String topicName = "joinDataTopic";
    private static String groupName = "joinDataGroupName";

    public static void main(String[] args) {
        System.out.println("send data to rocketmq");
        ProducerFromFile.produce("joinData-1.txt", NAMESRV_ADDRESS, topicName);

        ProducerFromFile.produce("joinData-2.txt", NAMESRV_ADDRESS, topicName + 2);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }

        System.out.println("begin streams code");

        DataStream leftStream = StreamBuilder.dataStream("namespace", "name").fromRocketmq(topicName, groupName, true, NAMESRV_ADDRESS).filter((JSONObject value) -> {
            if (value.getString("ProjectName") != null && value.getString("LogStore") != null) {
                return true;
            }
            return false;
        });

        DataStream rightStream = StreamBuilder.dataStream("namespace", "name2").fromRocketmq(topicName + 2, groupName + 2, true, NAMESRV_ADDRESS).filter((JSONObject value) -> {
            if (value.getString("ProjectName") != null && value.getString("LogStore") != null) {
                return true;
            }
            return false;
        });

        leftStream.leftJoin(rightStream).setCondition("(ProjectName,==,ProjectName)&(LogStore,==,LogStore)").toDataStream().toPrint(1).start();

        System.out.println("consumer end");
    }

}
