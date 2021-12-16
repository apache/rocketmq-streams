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

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("file://file_path/xxxxxxxx.json"));
        String line = bufferedReader.readLine();

        DefaultMQProducer producer = new DefaultMQProducer("producer_xxxx01");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        while (line != null) {
            Message msg = new Message("topic_xxxx01",
                "TagA",
                line.getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            producer.sendOneway(msg);
            line = bufferedReader.readLine();
        }

        Thread.sleep(5000);
        producer.shutdown();
    }
}
