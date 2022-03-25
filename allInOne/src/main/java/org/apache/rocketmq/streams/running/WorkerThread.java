package org.apache.rocketmq.streams.running;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.function.MapperAction;
import org.apache.rocketmq.streams.metadata.Data;
import org.apache.rocketmq.streams.topology.TopologyBuilder;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS;
import static org.apache.rocketmq.streams.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_GROUP;

public class WorkerThread extends Thread {
    private final TopologyBuilder topologyBuilder;
    private final Engine<?, ?, ?, ?> engine;

    public WorkerThread(TopologyBuilder topologyBuilder, String nameSrvAddr) throws MQClientException {
        this.topologyBuilder = topologyBuilder;

        RocketMQClient rocketMQClient = new RocketMQClient(nameSrvAddr);

        Set<String> sourceTopic = topologyBuilder.getSourceTopic();
        DefaultLitePullConsumer unionConsumer = rocketMQClient.pullConsumer(ROCKETMQ_STREAMS_CONSUMER_GROUP, sourceTopic);

        MessageQueueListener originListener = unionConsumer.getMessageQueueListener();
        MessageQueueListenerWrapper wrapper = new MessageQueueListenerWrapper(originListener, topologyBuilder);
        unionConsumer.setMessageQueueListener(wrapper);

        DefaultMQProducer producer = rocketMQClient.producer(ROCKETMQ_STREAMS_CONSUMER_GROUP);
        DefaultMQAdminExt mqAdmin = rocketMQClient.getMQAdmin();


        this.engine = new Engine<>(unionConsumer, producer, mqAdmin, wrapper::selectProcessor);
    }

    @Override
    public void run() {
        try {
            this.engine.start();

            this.engine.runInLoop();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            this.engine.stop();
        }
    }


    @SuppressWarnings("unchecked")
    static class Engine<K, V, OK, OV> {
        private final DefaultLitePullConsumer unionConsumer;
        private final DefaultMQProducer producer;
        private final DefaultMQAdminExt mqAdmin;
        private final MapperAction<String, Integer, Processor<K, V, OK, OV>> mapperAction;
        private volatile boolean running = false;

        public Engine(DefaultLitePullConsumer unionConsumer, DefaultMQProducer producer,
                      DefaultMQAdminExt mqAdmin,
                      MapperAction<String, Integer, Processor<K, V, OK, OV>> mapperAction) {
            this.unionConsumer = unionConsumer;
            this.producer = producer;
            this.mqAdmin = mqAdmin;
            this.mapperAction = mapperAction;
        }

        //todo 恢复状态


        //处理
        public void start() {
            try {
                this.unionConsumer.start();
                this.producer.start();
                this.mqAdmin.start();


            } catch (MQClientException e) {
                //todo
                e.printStackTrace();
            }
        }

        public void runInLoop() throws Throwable {
            if (running) {
                return;
            }

            running = true;
            //1、阻塞等待分配了哪些MQ
            //2、然后加载状态

            while (running) {
                List<MessageExt> list = this.unionConsumer.poll(0);
                if (list.size() == 0) {
                    Thread.sleep(100);
                    continue;
                }

                HashSet<MessageQueue> set = new HashSet<>();
                for (MessageExt messageExt : list) {
                    byte[] body = messageExt.getBody();
                    if (body == null || body.length == 0) {
                        continue;
                    }

                    //todo 将 body转化为V类型的对象，现在这里只支持String
                    V value = (V)Integer.valueOf(new String(body, StandardCharsets.UTF_8));

                    K key = null;
                    String temp = messageExt.getProperty(PROPERTY_KEYS);
                    if (!StringUtils.isEmpty(temp)) {
                        key = (K) temp;
                    }

                    Data<K, V> data = new Data<>(key, value);

                    String topic = messageExt.getTopic();
                    int queueId = messageExt.getQueueId();
                    String brokerName = messageExt.getBrokerName();
                    MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                    set.add(queue);


                    Processor<K, V, OK, OV> processor = this.mapperAction.convert(topic, queueId);

                    StreamContextImpl context = new StreamContextImpl(producer, mqAdmin, messageExt);
                    processor.preProcess(context);
                    processor.process(data);
                }

                //todo 每次都提交位点消耗太大，后面改成拉取消息放入buffer的形式。
                this.unionConsumer.commit(set, true);
            }
        }

        public void stop() {
            this.running = false;

            try {
                this.unionConsumer.shutdown();
                this.producer.shutdown();
                this.mqAdmin.shutdown();
            } catch (Throwable e) {
                //todo
                e.printStackTrace();
            }
        }
    }


}
