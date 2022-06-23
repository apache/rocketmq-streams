/*
 *
 *  * Licensed to the Apache Software Foundation (ASF) under one or more
 *  * contributor license agreements.  See the NOTICE file distributed with
 *  * this work for additional information regarding copyright ownership.
 *  * The ASF licenses this file to You under the Apache License, Version 2.0
 *  * (the "License"); you may not use this file except in compliance with
 *  * the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.rocketmq.streams.examples.mutilconsumer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.streams.examples.aggregate.ProducerFromFile;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.rocketmq.streams.examples.aggregate.Constant.NAMESRV_ADDRESS;

public class Producer {
    private static final AtomicInteger count = new AtomicInteger(0);

    /**
     * total produce 1000 data.
     *
     * @param fileName
     */
    public static void produceInLoop(String topic, String fileName) {
        DefaultMQProducer producer = new DefaultMQProducer("test-group");

        try {
            producer.setNamesrvAddr(NAMESRV_ADDRESS);
            producer.start();

            List<String> result = ProducerFromFile.read(fileName);

            for (int i = 0; i < 100; i++) {
                if (count.get() % 100 == 0) {
                    System.out.println("already send message: " + count.get());
                }

                for (String str : result) {
                    Message msg = new Message(topic, "", str.getBytes(RemotingHelper.DEFAULT_CHARSET));
                    producer.send(msg);
                    count.getAndIncrement();
                }

                Thread.sleep(100);
            }

        } catch (Throwable t) {

        }

    }

}
