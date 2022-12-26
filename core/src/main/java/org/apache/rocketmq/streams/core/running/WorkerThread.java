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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.runtime.operators.TimeType;
import org.apache.rocketmq.streams.core.state.RocketMQStore;
import org.apache.rocketmq.streams.core.state.RocksDBStore;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.RocketMQUtil;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import static org.apache.rocketmq.streams.core.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_GROUP;

public class WorkerThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class.getName());
    private final TopologyBuilder topologyBuilder;
    private final PlanetaryEngine<?, ?> planetaryEngine;
    private final Properties properties;
    private final String groupName = StreamConfig.getJobId() + "_" + ROCKETMQ_STREAMS_CONSUMER_GROUP;


    public WorkerThread(TopologyBuilder topologyBuilder, Properties properties) throws MQClientException {
        this.topologyBuilder = topologyBuilder;
        this.properties = properties;

        RocketMQClient rocketMQClient = new RocketMQClient(properties.getProperty(MixAll.NAMESRV_ADDR_PROPERTY));

        Set<String> topicNames = topologyBuilder.getSourceTopic();


        DefaultLitePullConsumer unionConsumer = rocketMQClient.pullConsumer(groupName, topicNames);

        MessageQueueListener originListener = unionConsumer.getMessageQueueListener();
        MessageQueueListenerWrapper wrapper = new MessageQueueListenerWrapper(originListener, topologyBuilder);
        unionConsumer.setMessageQueueListener(wrapper);

        DefaultMQProducer producer = rocketMQClient.producer(groupName);
        DefaultMQAdminExt mqAdmin = rocketMQClient.getMQAdmin();

        RocksDBStore rocksDBStore = new RocksDBStore();
        RocketMQStore store = new RocketMQStore(producer, rocksDBStore, mqAdmin, this.properties);

        this.planetaryEngine = new PlanetaryEngine<>(unionConsumer, producer, store, mqAdmin, wrapper);
    }

    @Override
    public void run() {
        try {
            this.planetaryEngine.start();

            this.planetaryEngine.runInLoop();
        } catch (Throwable e) {
            logger.error("planetaryEngine error.", e);
            throw new RuntimeException(e);
        } finally {
            logger.info("planetaryEngine stop.");
            this.planetaryEngine.stop();
        }
    }

    public void shutdown() {
        this.planetaryEngine.stop();
    }


    @SuppressWarnings("unchecked")
    class PlanetaryEngine<K, V> {
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
            this.wrapper.setRecoverHandler((addQueue, removeQueue) -> {
                try {
                    PlanetaryEngine.this.stateStore.recover(addQueue, removeQueue);
                    return null;
                } catch (Throwable e) {
                    logger.error("recover error.", e);
                    return e;
                }
            });
        }


        //处理
        void start() throws Throwable {
            createShuffleTopic();

            this.unionConsumer.start();
            this.producer.start();
            this.stateStore.init();
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
                    logger.debug("source topic queue:[{}]", queue);


                    String key = Utils.buildKey(brokerName, topic, queueId);
                    SourceSupplier.SourceProcessor<K, V> processor = (SourceSupplier.SourceProcessor<K, V>) wrapper.selectProcessor(key);

                    StreamContextImpl<V> context = new StreamContextImpl<>(producer, mqAdmin, stateStore, key);

                    processor.preProcess(context);

                    Pair<K, V> pair = processor.deserialize(keyClassName, valueClassName, body);

                    long timestamp;
                    String userProperty = messageExt.getUserProperty(Constant.SOURCE_TIMESTAMP);
                    if (!StringUtils.isEmpty(userProperty)) {
                        timestamp = Long.parseLong(userProperty);
                    } else {
                        timestamp = processor.getTimestamp(messageExt, (TimeType) properties.get(Constant.TIME_TYPE));
                    }

                    String delay = properties.getProperty(Constant.ALLOW_LATENESS_MILLISECOND, "0");
                    long watermark = processor.getWatermark(timestamp, Long.parseLong(delay));
                    context.setWatermark(watermark);

                    Data<K, V> data = new Data<>(pair.getKey(), pair.getValue(), timestamp, new Properties());
                    context.setKey(pair.getKey());
                    if (topic.contains(Constant.SHUFFLE_TOPIC_SUFFIX)) {
                        logger.debug("shuffle data: [{}]", data);
                    } else {
                        logger.debug("source data: [{}]", data);
                    }
                    context.forward(data);
                }

                //todo 每次都提交位点消耗太大，后面改成拉取消息放入buffer的形式。
                for (MessageQueue messageQueue : set) {
                    logger.debug("commit messageQueue: [{}]", messageQueue);
                }
                this.unionConsumer.commit(set, true);
                this.stateStore.persist(set);
                //todo 提交消费位点、写出sink数据、写出状态、需要保持原子
            }
        }


        void createShuffleTopic() throws Throwable {
            Set<String> total = WorkerThread.this.topologyBuilder.getSourceTopic();

            List<String> shuffleTopic = new ArrayList<>();

            for (String topic : total) {
                if (topic.endsWith(Constant.SHUFFLE_TOPIC_SUFFIX)) {
                    shuffleTopic.add(topic);
                }
            }

            for (String topicName : shuffleTopic) {
                RocketMQUtil.createStaticTopic(mqAdmin, topicName, StreamConfig.SHUFFLE_TOPIC_QUEUE_NUM);
            }
        }

        public void stop() {
            this.stop = true;

            try {
                this.unionConsumer.shutdown();
                this.producer.shutdown();
                this.mqAdmin.shutdown();
            } catch (Throwable e) {
                logger.error("error when stop.", e);
            }
        }
    }


}
