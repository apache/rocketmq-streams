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
import com.google.common.collect.Maps;
import java.util.Map;
import java.util.Random;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.junit.Test;

public class JoinTest {

    @Test
    public void testProduce() {
        DefaultMQProducer producer = new DefaultMQProducer("test_group");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        //Launch the instance.
        try {
            producer.start();
            for (int i = 0; i < 100; i++) {
                Random r = new Random();
                //Create a message instance, specifying topic, tag and message body.
                Map<String, Object> data = Maps.newHashMap();
                data.put("InFlow", r.nextInt(65535));
                data.put("ProjectName", "project-" + i);
                data.put("LogStore", "logstore-" + i);
                data.put("OutFlow", r.nextInt(65535));
                data.put("logTime", System.currentTimeMillis());
                String jsonData = JSONObject.toJSONString(data);
                Message msg = new Message("TopicTest" /* Topic */, "TagA" /* Tag */, jsonData.getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */);
                SendResult sendResult = producer.send(msg);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(5000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testInnerJoin() {
        DataStream left = StreamBuilder.dataStream("tmp", "tmp").fromFile("window_msg_10.txt", true);

        DataStream right = StreamBuilder.dataStream("tmp", "tmp2").fromFile("dim.txt", true);

        left.join(right).on("(ProjectName,=,project)").toDataStream().toPrint().start();

    }

    @Test
    public void testLeftDim() {
        DataStream left = StreamBuilder.dataStream("tmp", "tmp").fromFile("window_msg_10.txt", true);

        DataStream right = StreamBuilder.dataStream("tmp", "tmp2").fromFile("dim.txt", true);

        left.leftJoin(right).on("(ProjectName,=,project)").toDataStream().toPrint().start();
    }

    @Test
    public void testRocketmqJoin() {

        DataStream left = StreamBuilder.dataStream("tmp", "tmp").fromRocketmq("TopicTest", "groupA", true, "localhost:9876");
        //DataStream right = StreamBuilder.dataStream("tmp", "tmp2").fromFile("dim.txt", true);
        DataStream right = StreamBuilder.dataStream("tmp", "tmp").fromRocketmq("TopicTest", "groupB", true, "localhost:9876");

        left.join(right).on("(ProjectName,=,project)").toDataStream().toPrint().start();
    }

}
