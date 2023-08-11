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
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.exception.DataProcessThrowable;
import org.apache.rocketmq.streams.core.exception.RStreamsException;
import org.apache.rocketmq.streams.core.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.window.fire.IdleWindowScaner;
import org.apache.rocketmq.streams.core.window.TimeType;
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
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.rocketmq.streams.core.common.Constant.*;
import static org.apache.rocketmq.streams.core.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_FORM_WHERE;
import static org.apache.rocketmq.streams.core.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_GROUP;

public class WorkerThread extends Thread {
    private static final Logger logger = LoggerFactory.getLogger(WorkerThread.class.getName());
    private final TopologyBuilder topologyBuilder;
    private final PlanetaryEngine<?, ?> planetaryEngine;
    private final Properties properties;
    private final String jobId;
    private final ScheduledExecutorService executor;


    public WorkerThread(String threadName,
                        TopologyBuilder topologyBuilder,
                        Properties properties,
                        ScheduledExecutorService executor) throws MQClientException {
        super(threadName);

        this.topologyBuilder = topologyBuilder;
        this.properties = properties;
        jobId = topologyBuilder.getJobId();
        this.executor = executor;

        String groupName = String.join("_", jobId, ROCKETMQ_STREAMS_CONSUMER_GROUP);

        RocketMQClient rocketMQClient = new RocketMQClient(properties.getProperty(MixAll.NAMESRV_ADDR_PROPERTY));

        Set<String> topicNames = topologyBuilder.getSourceTopic();

        DefaultLitePullConsumer unionConsumer = rocketMQClient.pullConsumer(groupName, topicNames);

        MessageQueueListener originListener = unionConsumer.getMessageQueueListener();
        MessageQueueListenerWrapper wrapper = new MessageQueueListenerWrapper(originListener, topologyBuilder);
        unionConsumer.setMessageQueueListener(wrapper);

        DefaultMQProducer producer = rocketMQClient.producer(groupName);
        DefaultMQAdminExt mqAdmin = rocketMQClient.getMQAdmin();

        RocksDBStore rocksDBStore = new RocksDBStore(threadName);
        RocketMQStore store = new RocketMQStore(producer, rocksDBStore, mqAdmin, this.properties);

        this.planetaryEngine = new PlanetaryEngine<>(unionConsumer, producer, store, mqAdmin, wrapper, topicNames);
    }

    @Override
    public void run() {
        try {
            this.planetaryEngine.start();
            logger.info("worker thread=[{}], start task success, jobId:{}", this.getName(), jobId);

            this.planetaryEngine.maybeResetOffsetToFirst();
            this.planetaryEngine.runInLoop();
        } catch (Throwable e) {
            logger.error("worker thread=[{}], error:{}.", this.getName(), e);
            throw new RStreamsException(e);
        } finally {
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
        private final IdleWindowScaner idleWindowScaner;
        private volatile boolean stop = false;

        private Set<String> sourceTopicSet;

        private final HashSet<MessageQueue> mq2Commit = new HashSet<>();


        public PlanetaryEngine(DefaultLitePullConsumer unionConsumer, DefaultMQProducer producer, StateStore stateStore,
                               DefaultMQAdminExt mqAdmin, MessageQueueListenerWrapper wrapper, Set<String> sourceTopicSet) {
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
            this.sourceTopicSet = sourceTopicSet;

            Integer idleTime = (Integer) WorkerThread.this.properties.getOrDefault(StreamConfig.IDLE_TIME_TO_FIRE_WINDOW, 2000);
            int commitInterval = (Integer) WorkerThread.this.properties.getOrDefault(StreamConfig.COMMIT_STATE_INTERNAL_MS, 2 * 1000);
            this.idleWindowScaner = new IdleWindowScaner(idleTime, executor);
            WorkerThread.this.executor.scheduleAtFixedRate(() -> {
                try {
                    doCommit(mq2Commit);
                } catch (Throwable t) {
                    logger.error("commit offset and state error.", t);
                }
            }, 1000, commitInterval, TimeUnit.MILLISECONDS);
        }


        void start() throws Throwable {
            createShuffleTopic();

            this.unionConsumer.start();
            this.producer.start();
            this.stateStore.init();
        }

        void runInLoop() throws Throwable {
            while (!stop) {
                try {
                    List<MessageExt> list = this.unionConsumer.poll(10);
                    for (MessageExt messageExt : list) {
                        byte[] body = messageExt.getBody();
                        if (body == null || body.length == 0) {
                            break;
                        }

                        String keyClassName = messageExt.getUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME);
                        String valueClassName = messageExt.getUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME);

                        String topic = messageExt.getTopic();
                        int queueId = messageExt.getQueueId();
                        String brokerName = messageExt.getBrokerName();
                        MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                        mq2Commit.add(queue);
                        logger.debug("source topic queue:[{}]", queue);


                        String key = Utils.buildKey(brokerName, topic, queueId);
                        SourceSupplier.SourceProcessor<K, V> processor = (SourceSupplier.SourceProcessor<K, V>) wrapper.selectProcessor(key);

                        StreamContextImpl<V> context = new StreamContextImpl<>(properties, producer, mqAdmin, stateStore, key, idleWindowScaner);

                        processor.preProcess(context);

                        Pair<K, V> pair = processor.deserialize(keyClassName, valueClassName, body);

                        long timestamp = prepareTime(messageExt, processor);

                        Data<K, V> data = new Data<>(pair.getKey(), pair.getValue(), timestamp, new Properties());
                        context.setKey(pair.getKey());
                        if (topic.contains(Constant.SHUFFLE_TOPIC_SUFFIX)) {
                            logger.debug("shuffle data: [{}]", data);
                        } else {
                            logger.debug("source data: [{}]", data);
                        }

                        try {
                            context.forward(data);
                        } catch (Throwable t) {
                            logger.error("process error.", t);
                            throw new DataProcessThrowable(t);
                        }
                    }

                } catch (Throwable t) {
                    Object skipDataError = properties.getOrDefault(Constant.SKIP_DATA_ERROR, Boolean.TRUE);
                    if (skipDataError == Boolean.TRUE) {
                        logger.error("ignore error, jobId=[{}], skip this data.", topologyBuilder.getJobId(), t);
                        //ignored
                    } else {
                        throw t;
                    }
                }
            }
        }

        void doCommit(HashSet<MessageQueue> set) throws Throwable {
            if (set != null && set.size() != 0) {

                this.stateStore.persist(set);
                this.unionConsumer.commit(set, true);

                for (MessageQueue messageQueue : set) {
                    logger.debug("committed messageQueue: [{}]", messageQueue);
                }

                set.clear();
            }
        }

        void maybeResetOffsetToFirst() throws Exception {
            ConsumeFromWhere consumeFromWhere = (ConsumeFromWhere) properties.getOrDefault(ROCKETMQ_STREAMS_CONSUMER_FORM_WHERE, ConsumeFromWhere.CONSUME_FROM_LAST_OFFSET);

            if (!consumeFromWhere.equals(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET)) {
                return;
            }

            for (String topic : sourceTopicSet) {
                // 内部 topic 不能重置位点
                if (topic.endsWith(Constant.SHUFFLE_TOPIC_SUFFIX) || topic.endsWith(STATE_TOPIC_SUFFIX)) {
                    continue;
                }
                ConsumeStats consumeStats = mqAdmin.examineConsumeStats(unionConsumer.getConsumerGroup(), topic);
                Map<MessageQueue, OffsetWrapper> offsetTable = consumeStats.getOffsetTable();
                Set<MessageQueue> messageQueues = offsetTable.keySet();
                for (MessageQueue messageQueue : messageQueues) {
                    try {
                        // 如果有消费进度，说明已经开始消费，跳过重置其消费进度
                        if (offsetTable.containsKey(messageQueue) &&
                                offsetTable.get(messageQueue).getConsumerOffset() != DEFAULT_CONSUME_OFFSET) {
                            break;
                        }

                        Long minOffset = mqAdmin.minOffset(messageQueue);
                        String brokerName = messageQueue.getBrokerName();
                        ClusterInfo clusterInfo = mqAdmin.examineBrokerClusterInfo();
                        BrokerData brokerData = clusterInfo.getBrokerAddrTable().get(brokerName);
                        if (brokerData == null) {
                            String msg = String.format("get broker error, have no broker info (name:%s)", brokerName);
                            logger.error(msg);
                            throw new RStreamsException(msg);
                        }
                        for (String brokerAddress : brokerData.getBrokerAddrs().values()) {
                            mqAdmin.resetOffsetByQueueId(brokerAddress,
                                    unionConsumer.getConsumerGroup(),
                                    messageQueue.getTopic(),
                                    messageQueue.getQueueId(),
                                    minOffset);
                        }
                    } catch (Exception e) {
                        logger.error("reset messageQueue:{} consumer offset to first failed.", messageQueue, e);
                        throw e;
                    }
                }
            }
        }


        long prepareTime(MessageExt messageExt, SourceSupplier.SourceProcessor<K, V> processor) {
            TimeType type = (TimeType) properties.get(StreamConfig.TIME_TYPE);

            long timestamp;
            String userProperty = messageExt.getUserProperty(Constant.SOURCE_TIMESTAMP);
            if (!StringUtils.isEmpty(userProperty)) {
                //data come from shuffle topic
                timestamp = Long.parseLong(userProperty);
            } else {
                //data come from user source topic
                timestamp = processor.getTimestamp(messageExt, type);
            }

            return timestamp;
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

        public synchronized void stop() {
            if (this.stop) {
                return;
            }

            this.stop = true;

            try {
                this.unionConsumer.shutdown();

                this.stateStore.close();
                this.idleWindowScaner.close();

                this.producer.shutdown();
                this.mqAdmin.shutdown();
                logger.info("shutdown engine success, thread:{}, jobId:{}", WorkerThread.this.getName(), jobId);
            } catch (Throwable e) {
                logger.error("error when stop engin.", e);
                throw new RStreamsException(e);
            }
        }
    }


}
