package org.apache.rocketmq.streams.core.state;
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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.runtime.operators.WindowKey;
import org.apache.rocketmq.streams.core.serialization.ShuffleProtocol;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class RocketMQStore extends AbstractStore implements StateStore {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQStore.class.getName());
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final RocksDBStore rocksDBStore;
    private final Properties properties;

    private final ExecutorService executor = Executors.newFixedThreadPool(4);
    private final ShuffleProtocol protocol = new ShuffleProtocol();

    private final ConcurrentHashMap<MessageQueue/*messageQueue of state topic*/, CountDownLatch2> recoveringQueueMutex = new ConcurrentHashMap<>();

    public RocketMQStore(DefaultMQProducer producer, RocksDBStore rocksDBStore, DefaultMQAdminExt mqAdmin, Properties properties) {
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.rocksDBStore = rocksDBStore;
        this.properties = properties;
    }

    @Override
    public void init() throws Throwable {
    }

    @Override
    public void recover(Set<MessageQueue> addQueues, Set<MessageQueue> removeQueues) throws Throwable {
        this.loadState(addQueues);
        this.removeState(removeQueues);
    }

    @Override
    public void waitIfNotReady(MessageQueue messageQueue) throws Throwable {
        MessageQueue stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(messageQueue);
        CountDownLatch2 waitPoint = this.recoveringQueueMutex.get(stateTopicQueue);

        long start = 0;
        long end = 0;
        try {
            start = System.currentTimeMillis();
            waitPoint.await(5000, TimeUnit.MILLISECONDS);
            end = System.currentTimeMillis();
        } finally {
            long cost = end - start;
            if (cost > 2000) {
                logger.error("recover finish, consume time:" + cost + " ms.");
            }
        }
    }




    @Override
    public byte[] get(byte[] key) throws Throwable {
        if (key == null || key.length == 0) {
            return new byte[0];
        }
        return this.rocksDBStore.get(key);
    }


    @Override
    public void put(MessageQueue stateTopicMessageQueue, byte[] key, byte[] value) throws Throwable {
        String stateTopicQueueKey = buildKey(stateTopicMessageQueue);
        super.putInCalculating(stateTopicQueueKey, key);
        this.rocksDBStore.put(key, value);
    }



    @Override
    public List<Pair<byte[], byte[]>> searchStateLessThanWatermark(String keyPrefix, long lessThanThisTime, ValueMapperAction<byte[], WindowKey> deserializer) throws Throwable {
        if (StringUtils.isEmpty(keyPrefix)) {
            return new ArrayList<>();
        }

        return this.rocksDBStore.searchStateLessThanWatermark(keyPrefix, lessThanThisTime, deserializer);
    }

    @Override
    public void delete(byte[] key) throws Throwable {
        if (key == null || key.length == 0) {
            return;
        }
        //删除远程
        String stateTopicQueue = super.whichStateTopicQueueBelongTo(key);
        String[] split = Utils.split(stateTopicQueue);
        String topic = split[1];
        MessageQueue queue = new MessageQueue(split[1], split[0], Integer.parseInt(split[2]));

        Message message = new Message(topic, Constant.EMPTY_BODY.getBytes(StandardCharsets.UTF_8));
        message.setKeys(Utils.toHexString(key));
        message.putUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME, key.getClass().getName());
        message.putUserProperty(Constant.EMPTY_BODY, Constant.TRUE);
        producer.send(message, queue);

        //删除rocksdb
        this.rocksDBStore.deleteByKey(key);

        //删除内存中的key
        super.removeAllKey(key);

        try {
            logger.debug("delete key: " + new String(key, StandardCharsets.UTF_8) + ",MessageQueue: " + queue);
        } catch (Throwable t) {
        }
    }

    @Override
    public void persist(Set<MessageQueue> messageQueues) throws Throwable {
        if (messageQueues == null || messageQueues.size() == 0) {
            return;
        }

        Set<MessageQueue> stateTopicQueues = convertSourceTopicQueue2StateTopicQueue(messageQueues);
        for (MessageQueue stateTopicQueue : stateTopicQueues) {
            String stateTopicQueueKey = buildKey(stateTopicQueue);
            Set<byte[]> keySet = super.getInCalculating(stateTopicQueueKey);

            if (keySet == null || keySet.size() == 0) {
                return;
            }

            String stateTopic = stateTopicQueue.getTopic();
            createStateTopicIfNotExist(stateTopic);

            for (byte[] key : keySet) {

                byte[] valueBytes = this.rocksDBStore.get(key);
                if (valueBytes == null) {
                    continue;
                }

                byte[] body = this.protocol.merge(key, valueBytes);

                Message message = new Message(stateTopicQueue.getTopic(), body);
                message.setKeys(Utils.toHexString(key));

                try {
                    logger.debug("persist key: " + new String(key, StandardCharsets.UTF_8) + ",messageQueue: " + stateTopicQueue);
                } catch (Throwable t) {
                }

                this.producer.send(message, stateTopicQueue);
            }
            super.removeCalculating(stateTopicQueueKey);
        }
    }

    public void loadState(Set<MessageQueue> addQueues) throws Throwable {
        if (addQueues == null || addQueues.size() == 0) {
            return;
        }

        final DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(StreamConfig.ROCKETMQ_STREAMS_STATE_CONSUMER_GROUP);
        consumer.setNamesrvAddr(properties.getProperty(MixAll.NAMESRV_ADDR_PROPERTY));
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        consumer.setAutoCommit(false);
        consumer.start();

        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(addQueues);
        for (MessageQueue messageQueue : stateTopicQueue) {
            createStateTopicIfNotExist(messageQueue.getTopic());
        }

        consumer.assign(stateTopicQueue);
        for (MessageQueue queue : stateTopicQueue) {
            consumer.seekToBegin(queue);
        }

        Future<?> future = this.executor.submit(() -> {
            try {
                pullToLast(consumer);
            } catch (Throwable e) {
                logger.error("pull to last error.", e);
                throw new RuntimeException(e);
            } finally {
                consumer.shutdown();
            }
        });

        try {
            future.get(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
        }
    }

    public void removeState(Set<MessageQueue> removeQueues) throws Throwable {
        if (removeQueues == null || removeQueues.size() == 0) {
            return;
        }

        Future<?> future = this.executor.submit(() -> {
            try {
                if (removeQueues.size() == 0) {
                    return;
                }
                Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(removeQueues);

                Map<String/*brokerName@topic@queueId*/, List<MessageQueue>> groupByUniqueQueue = stateTopicQueue.stream().parallel().collect(Collectors.groupingBy(this::buildKey));
                for (String stateUniqueQueue : groupByUniqueQueue.keySet()) {
                    Set<byte[]> stateTopicQueueKey = super.getAll(stateUniqueQueue);
                    for (byte[] key : stateTopicQueueKey) {
                        this.rocksDBStore.deleteByKey(key);
                    }
                    super.removeAll(stateUniqueQueue);
                }


                for (MessageQueue stateMessageQueue : stateTopicQueue) {
                    this.recoveringQueueMutex.remove(stateMessageQueue);
                }
            } catch (Throwable e) {
                logger.error("remove state error", e);
                throw new RuntimeException(e);
            }
        });

        try {
            future.get(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | TimeoutException e) {
        }
    }

    private void pullToLast(DefaultLitePullConsumer consumer) throws Throwable {
        Set<MessageQueue> readyToRecover = consumer.assignment();
        for (MessageQueue messageQueue : readyToRecover) {
            this.recoveringQueueMutex.computeIfAbsent(messageQueue, messageQueue1 -> new CountDownLatch2(1));
        }

        List<MessageExt> holder = new ArrayList<>();
        //recover
        List<MessageExt> result = consumer.poll(50);
        while (result != null && result.size() != 0) {
            holder.addAll(result);
            if (holder.size() <= 1000) {
                result = consumer.poll(50);
                continue;
            }

            replayState(holder);
            holder.clear();

            result = consumer.poll(50);
        }
        if (holder.size() != 0) {
            replayState(holder);
        }

        //恢复完毕；
        Set<MessageQueue> recoverOver = consumer.assignment();
        for (MessageQueue messageQueue : recoverOver) {
            CountDownLatch2 waitPoint = this.recoveringQueueMutex.get(messageQueue);
            waitPoint.countDown();
        }
    }

    //拉的数据越多，重放效率越高,
    // 能保证一个q里面后面pull到的数据queueOffset一定比前一批次拉取的queueOffset大吗？
    private void replayState(List<MessageExt> msgs) throws Throwable {
        if (msgs == null || msgs.size() == 0) {
            return;
        }

        Map<String/*brokerName@topic@queueId of state topic*/, List<MessageExt>> groupByQueueId = msgs.stream().parallel().collect(Collectors.groupingBy(this::buildKey));

        for (String uniqueQueue : groupByQueueId.keySet()) {
            List<MessageExt> messageExts = groupByQueueId.get(uniqueQueue);
            Map<String/*K的hashcode，真正的key在body里面*/, List<MessageExt>> groupByKeyHashcode = messageExts.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));

            for (String keyHashcode : groupByKeyHashcode.keySet()) {
                //相同brokerName@topic@queueId + keyHashcode 在一次拉取中的所有数据
                List<MessageExt> exts = groupByKeyHashcode.get(keyHashcode);

                //重放，按照queueOffset，相同key，大的queueOffset覆盖小的queueOffset
                List<MessageExt> sortedMessages = sortByQueueOffset(exts);

                //最后的消息
                MessageExt result = sortedMessages.get(sortedMessages.size() - 1);

                String emptyBody = result.getUserProperty(Constant.EMPTY_BODY);
                if (Constant.TRUE.equals(emptyBody)) {
                    continue;
                }

                byte[] body = result.getBody();
                Pair<byte[], byte[]> pair = this.protocol.split(body);

                byte[] key = pair.getKey();
                byte[] value = pair.getValue();

                //放入rocksdb
                MessageQueue stateTopicQueue = new MessageQueue(result.getTopic(), result.getBrokerName(), result.getQueueId());
                try {
                    logger.debug("recover state, key: " + new String(key, StandardCharsets.UTF_8) + ", stateTopicQueue: " + stateTopicQueue);
                } catch (Throwable t) {
                }

                String stateTopicQueueKey = buildKey(stateTopicQueue);
                super.putInRecover(stateTopicQueueKey, key);
                this.rocksDBStore.put(key, value);
            }
        }
    }


    private List<MessageExt> sortByQueueOffset(List<MessageExt> target) {
        if (target == null || target.size() == 0) {
            return new ArrayList<>();
        }

        target.sort((o1, o2) -> {
            long diff = o1.getQueueOffset() - o2.getQueueOffset();

            if (diff > 0) {
                return 1;
            }

            if (diff < 0) {
                return -1;
            }
            return 0;
        });

        return target;
    }


    private final List<String> existStateTopic = new ArrayList<>();

    private void createStateTopicIfNotExist(String stateTopic) {
        String sourceTopic = stateTopic2SourceTopic(stateTopic);

        if (existStateTopic.contains(stateTopic)) {
            return;
        }

        //检查是否存在
        try {
            mqAdmin.examineTopicRouteInfo(stateTopic);
            existStateTopic.add(stateTopic);
            return;
        } catch (RemotingException | InterruptedException e) {
            logger.error("examine state topic route info error.", e);
            throw new RuntimeException("examine state topic route info error.", e);
        } catch (MQClientException exception) {
            if (exception.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                logger.info("state topic does not exist, create it.");
            } else {
                throw new RuntimeException(exception);
            }
        }

        //创建
        try {

            //找到brokerAddr
            TopicRouteData topicRouteData = mqAdmin.examineTopicRouteInfo(sourceTopic);
            List<QueueData> queueData = topicRouteData.getQueueDatas();
            List<BrokerData> brokerData = topicRouteData.getBrokerDatas();


            HashMap<String, String> brokerName2MaterBrokerAddr = new HashMap<>();
            for (BrokerData broker : brokerData) {
                String masterBrokerAddr = broker.getBrokerAddrs().get(0L);
                brokerName2MaterBrokerAddr.put(broker.getBrokerName(), masterBrokerAddr);
            }

            for (QueueData queue : queueData) {
                int readQueueNums = queue.getReadQueueNums();
                int writeQueueNums = queue.getWriteQueueNums();
                String brokerName = queue.getBrokerName();

                TopicConfig topicConfig = new TopicConfig(stateTopic, readQueueNums, writeQueueNums);

                HashMap<String, String> temp = new HashMap<>();
                //todo 暂时不能支持；
//                temp.put("+delete.policy", "COMPACTION");
                topicConfig.setAttributes(temp);

                mqAdmin.createAndUpdateTopicConfig(brokerName2MaterBrokerAddr.get(brokerName), topicConfig);
            }

            existStateTopic.add(stateTopic);
        } catch (Throwable t) {
            logger.error("create state topic error.");
            throw new RuntimeException("create state topic error.", t);
        }

    }

    @Override
    public void close() throws Exception {

    }
}
