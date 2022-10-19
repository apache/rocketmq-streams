package org.apache.rocketmq.streams.core.running;
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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.core.state.DefaultStore;
import org.apache.rocketmq.streams.core.state.RocksDBStore;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.rocketmq.streams.core.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_GROUP;

public class WorkerThread extends Thread {
    private final static InternalLogger log = ClientLogger.getLog();
    private final TopologyBuilder topologyBuilder;
    private final PlanetaryEngine<?, ?> planetaryEngine;
    private final Properties properties;

    public WorkerThread(TopologyBuilder topologyBuilder, Properties properties) throws MQClientException {
        this.topologyBuilder = topologyBuilder;
        this.properties = properties;

        RocketMQClient rocketMQClient = new RocketMQClient(properties.getProperty(MixAll.NAMESRV_ADDR_PROPERTY));

        Set<String> sourceTopic = topologyBuilder.getSourceTopic();
        DefaultLitePullConsumer unionConsumer = rocketMQClient.pullConsumer(ROCKETMQ_STREAMS_CONSUMER_GROUP, sourceTopic);

        MessageQueueListener originListener = unionConsumer.getMessageQueueListener();
        MessageQueueListenerWrapper wrapper = new MessageQueueListenerWrapper(originListener, topologyBuilder);
        unionConsumer.setMessageQueueListener(wrapper);

        DefaultMQProducer producer = rocketMQClient.producer(ROCKETMQ_STREAMS_CONSUMER_GROUP);
        DefaultMQAdminExt mqAdmin = rocketMQClient.getMQAdmin();

        RocksDBStore rocksDBStore = new RocksDBStore();
        DefaultStore store = new DefaultStore(rocksDBStore);

        this.planetaryEngine = new PlanetaryEngine<>(unionConsumer, producer, store, mqAdmin, wrapper);
    }

    @Override
    public void run() {
        try {
            this.planetaryEngine.start();

            this.planetaryEngine.runInLoop();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            this.planetaryEngine.stop();
        }
    }

    public void shutdown() {
        this.planetaryEngine.stop();
    }


    @SuppressWarnings("unchecked")
    static class PlanetaryEngine<K, V> {
        private final DefaultLitePullConsumer unionConsumer;
        private final DefaultMQProducer producer;
        private final DefaultMQAdminExt mqAdmin;
        private final StateStore stateStore;
        private final MessageQueueListenerWrapper wrapper;
        private volatile boolean stop = false;

        public PlanetaryEngine(DefaultLitePullConsumer unionConsumer, DefaultMQProducer producer, StateStore stateStore,
                               DefaultMQAdminExt mqAdmin, MessageQueueListenerWrapper wrapper) {
            this.unionConsumer = unionConsumer;
            this.producer = producer;
            this.mqAdmin = mqAdmin;
            this.stateStore = stateStore;
            this.wrapper = wrapper;
            this.wrapper.setRemoveQueueHandler(this::removeState);
            this.wrapper.setAddQueueHandler(this::loadState);
        }

        //todo 恢复状态
        public void loadState(Set<MessageQueue> addQueues) {
            this.stateStore.loadState(addQueues);
        }

        public void removeState(Set<MessageQueue> removeQueues) {
            this.stateStore.removeState(removeQueues);
        }

        //处理
        void start() {
            try {
                this.unionConsumer.start();
                this.producer.start();
                this.mqAdmin.start();
                this.stateStore.init();
            } catch (MQClientException e) {
                //todo
                e.printStackTrace();
            }
        }

        void runInLoop() throws Throwable {
            while (!stop) {
                List<MessageExt> list = this.unionConsumer.poll(0);
                if (list.size() == 0) {
                    Thread.sleep(10);
                    continue;
                }

                HashSet<MessageQueue> set = new HashSet<>();
                for (MessageExt messageExt : list) {
                    byte[] body = messageExt.getBody();
                    if (body == null || body.length == 0) {
                        continue;
                    }

                    String keyClassName = messageExt.getUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME);
                    String valueClassName = messageExt.getUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME);

                    String topic = messageExt.getTopic();
                    int queueId = messageExt.getQueueId();
                    String brokerName = messageExt.getBrokerName();
                    MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                    set.add(queue);


                    String key = wrapper.buildKey(brokerName, topic, queueId);
                    SourceSupplier.SourceProcessor<K, V> processor = (SourceSupplier.SourceProcessor<K, V>) wrapper.selectProcessor(key);

                    StreamContext<V> context = new StreamContextImpl<>(producer, mqAdmin, stateStore, messageExt);
                    context.getAdditional().put(Constant.SHUFFLE_KEY_CLASS_NAME, keyClassName);
                    context.getAdditional().put(Constant.SHUFFLE_VALUE_CLASS_NAME, valueClassName);

                    processor.preProcess(context);

                    Pair<K, V> pair = processor.deserialize(body);
                    context.setKey(pair.getObject1());

                    processor.process(pair.getObject2());
                }

                //todo 每次都提交位点消耗太大，后面改成拉取消息放入buffer的形式。
                this.unionConsumer.commit(set, true);
            }
        }


        public void stop() {
            this.stop = true;

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
