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
package org.apache.rocketmq.streams.core.function.supplier;


import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.selector.SelectMessageQueueByHash;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class SinkSupplier<K, T> implements Supplier<Processor<T>> {
    private static final Logger logger = LoggerFactory.getLogger(SinkSupplier.class);

    private final String topicName;
    private final KeyValueSerializer<K, T> serializer;

    public SinkSupplier(String topicName, KeyValueSerializer<K, T> serializer) {
        this.topicName = topicName;
        this.serializer = serializer;
    }

    @Override
    public Processor<T> get() {
        return new SinkProcessor(this.topicName, this.serializer);
    }

    private class SinkProcessor extends AbstractProcessor<T> {
        private final String topicName;
        private DefaultMQProducer producer;
        private final KeyValueSerializer<K, T> serializer;
        private K key;

        public SinkProcessor(String topicName, KeyValueSerializer<K, T> serializer) {
            this.topicName = topicName;
            this.serializer = serializer;
        }

        @Override
        public void preProcess(StreamContext<T> context) {
            this.context = context;
            this.producer = context.getDefaultMQProducer();
            this.key = context.getKey();
        }

        //sink into shuffle topic/state topic/user topic
        @Override
        public void process(T data) throws Throwable {
            if (data != null) {
                byte[] value = this.serializer.serialize(key, data);
                if (value == null || value.length == 0) {
                    //目前RocketMQ不支持发送body为null的消息；
                    return;
                }

                Message message;

                if (this.key == null) {
                    message = new Message(this.topicName, value);
                    message.putUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME, data.getClass().getName());
                    if (this.topicName.contains(Constant.SHUFFLE_TOPIC_SUFFIX)) {
                        message.putUserProperty(Constant.SOURCE_TIMESTAMP, String.valueOf(this.context.getDataTime()));
                    }

                    producer.send(message);
                } else {
                    message = new Message(this.topicName, value);
                    String hexKey = Utils.toHexString(this.key);
                    //the real key is in the body, this key is used to route the same key into the same queue.
                    message.setKeys(hexKey);


                    message.putUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME, this.key.getClass().getName());
                    message.putUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME, data.getClass().getName());

                    if (this.topicName.contains(Constant.SHUFFLE_TOPIC_SUFFIX)) {
                        message.putUserProperty(Constant.SOURCE_TIMESTAMP, String.valueOf(this.context.getDataTime()));
                    }

                    producer.send(message, new SelectMessageQueueByHash(), hexKey);
                }
            }
        }


    }
}
