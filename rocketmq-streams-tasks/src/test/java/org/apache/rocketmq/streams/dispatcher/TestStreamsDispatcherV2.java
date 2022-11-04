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
package org.apache.rocketmq.streams.dispatcher;

import java.util.List;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Before;
import org.junit.Test;

public class TestStreamsDispatcherV2 {

    @Before
    public void init() {
        try {
            DefaultMQPushConsumer defaultMQPushConsumer = new DefaultMQPushConsumer("test");
            defaultMQPushConsumer.setNamesrvAddr("localhost:9876");
            defaultMQPushConsumer.setConsumerGroup("please_rename_unique_group_name_4");
            defaultMQPushConsumer.subscribe("TopicTest", "*");
            defaultMQPushConsumer.setInstanceName("instance_1");
            defaultMQPushConsumer.registerMessageListener(new MessageListenerOrderly() {
                @Override public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
                    return null;
                }
            });
            defaultMQPushConsumer.start();

        } catch (MQClientException e) {
            throw new RuntimeException(e);
        }
    }
    @Test
    public void testDispatcher(){
        System.out.println("hello world");
    }



}
