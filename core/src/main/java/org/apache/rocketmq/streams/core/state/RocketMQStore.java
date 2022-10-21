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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.CountDownLatch2;
import org.apache.rocketmq.common.Pair;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.QueueData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.serialization.deImpl.KVJsonDeserializer;
import org.apache.rocketmq.streams.core.serialization.serImpl.KVJsonSerializer;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class RocketMQStore extends AbstractStore {
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final RocksDBStore rocksDBStore;

    private final ExecutorService executor = Executors.newFixedThreadPool(5);
    private final KVJsonDeserializer<?, ?> kvJsonDeserializer = new KVJsonDeserializer<>();
    private final KVJsonSerializer<Object, Object> kvJsonSerializer = new KVJsonSerializer<>();

    private final ConcurrentHashMap<MessageQueue/*messageQueue of state topic*/, CountDownLatch2> recoveringQueueMutex = new ConcurrentHashMap<>();

    public RocketMQStore(DefaultMQProducer producer, RocksDBStore rocksDBStore, DefaultMQAdminExt mqAdmin) {
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.rocksDBStore = rocksDBStore;
    }

    @Override
    public void init() throws Throwable {
        synchronized (lock) {
            if (state == StoreState.UNINITIALIZED) {
                synchronized (lock) {
                    this.rocksDBStore.init();
                    state = StoreState.INITIALIZED;
                }
            }
        }
    }

    @Override
    public void recover(Set<MessageQueue> addQueues, Set<MessageQueue> removeQueues) throws Throwable {
        this.loadState(addQueues);
        this.removeState(removeQueues);
    }

    @Override
    public void waitIfNotReady(MessageQueue messageQueue, Object key) throws Throwable {
        this.rocksDBStore.waitIfNotReady(messageQueue, key);

        CountDownLatch2 waitPoint = this.recoveringQueueMutex.get(messageQueue);
        waitPoint.await();
    }


    public void loadState(Set<MessageQueue> addQueues) throws Throwable {
        if (addQueues == null || addQueues.size() == 0) {
            return;
        }

        this.executor.submit(() -> {
            DefaultLitePullConsumer consumer = null;
            try {
                consumer = new DefaultLitePullConsumer(StreamConfig.ROCKETMQ_STREAMS_STATE_CONSUMER_GROUP);
                consumer.start();

                Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(addQueues);

                consumer.assign(stateTopicQueue);
                for (MessageQueue queue : stateTopicQueue) {
                    consumer.seekToBegin(queue);
                }

                pullToLast(consumer);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            } finally {
                if (consumer != null) {
                    consumer.shutdown();
                }
            }
        });
    }

    public void removeState(Set<MessageQueue> removeQueues) throws Throwable {
        this.executor.submit(() -> {
            try {
                this.rocksDBStore.removeState(removeQueues);

                Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(removeQueues);
                for (MessageQueue stateMessageQueue : stateTopicQueue) {
                    this.recoveringQueueMutex.remove(stateMessageQueue);
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public <K, V> V get(K key) {
        return this.rocksDBStore.get(key);
    }

    @Override
    public <K, V> void put(K k, V v) {
        this.rocksDBStore.put(k, v);
    }


    private void pullToLast(DefaultLitePullConsumer consumer) throws Throwable {
        Set<MessageQueue> readyToRecover = consumer.assignment();
        for (MessageQueue messageQueue : readyToRecover) {
            this.recoveringQueueMutex.computeIfAbsent(messageQueue, messageQueue1 -> new CountDownLatch2(1));
        }

        //recover
        List<MessageExt> result = consumer.poll(50);
        while (result != null && result.size() != 0) {
            replayState(result);
            result = consumer.poll(50);
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

                String keyClassName = result.getUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME);
                String valueClassName = result.getUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME);

                byte[] body = result.getBody();

                kvJsonDeserializer.configure(keyClassName, valueClassName);
                Pair<?, ?> pair = kvJsonDeserializer.deserialize(body);

                Object key = pair.getObject1();
                Object value = pair.getObject2();

                //放入rocksdb
                this.rocksDBStore.put(key, value);
                //放入索引 queue-key的索引
                ConcurrentHashMap<String, Set<Object>> stateTopicQueue2RocksDBKey = this.rocksDBStore.getStateTopicQueue2RocksDBKey();
                Set<Object> keySet = stateTopicQueue2RocksDBKey.computeIfAbsent(uniqueQueue, s -> new HashSet<>());
                keySet.add(key);
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

    @Override
    public void persist(Set<MessageQueue> messageQueues) throws Throwable {
        if (messageQueues == null || messageQueues.size() == 0) {
            return;
        }

        ConcurrentHashMap<String, Set<Object>> stateTopicQueue2RocksDBKey = this.rocksDBStore.getStateTopicQueue2RocksDBKey();

        Set<MessageQueue> stateTopicQueues = convertSourceTopicQueue2StateTopicQueue(messageQueues);
        for (MessageQueue stateTopicQueue : stateTopicQueues) {
            String key = buildKey(stateTopicQueue);

            Set<Object> rocketDBKeySet = stateTopicQueue2RocksDBKey.get(key);

            if (rocketDBKeySet == null || rocketDBKeySet.size() == 0) {
                return;
            }

            createStateTopicIfNotExist(stateTopicQueue);

            for (Object rocketDBKey : rocketDBKeySet) {
                Object value = this.rocksDBStore.get(rocketDBKey);
                if (value == null) {
                    continue;
                }

                byte[] body = kvJsonSerializer.serialize(rocketDBKey, value);

                Message message = new Message(stateTopicQueue.getTopic(), body);
                //todo 改进key的计算方式
                message.setKeys(String.valueOf(rocketDBKey.hashCode()));

                message.putUserProperty(Constant.SHUFFLE_KEY_CLASS_NAME, rocketDBKey.getClass().getName());
                message.putUserProperty(Constant.SHUFFLE_VALUE_CLASS_NAME, value.getClass().getName());


                this.producer.send(message, stateTopicQueue);
            }

        }
    }

    private final List<String> existTopic = new ArrayList<>();

    private void createStateTopicIfNotExist(MessageQueue stateTopicMessageQueue) {
        String sourceTopic = stateTopicMessageQueue.getTopic();
        String stateTopic = sourceTopic + STATE_TOPIC_SUFFIX;

        if (existTopic.contains(stateTopic)) {
            return;
        }

        //检查是否存在
        try {
            mqAdmin.examineTopicRouteInfo(stateTopic);
            existTopic.add(stateTopic);
            return;
        } catch (RemotingException | InterruptedException e) {
            throw new RuntimeException("examine state topic route info error.", e);
        } catch (MQClientException exception) {
            if (exception.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                System.out.println("state topic does not exist.");
            } else {
                throw new RuntimeException(exception);
            }
        }

        //创建
        try {
            TopicRouteData topicRouteData = mqAdmin.examineTopicRouteInfo(sourceTopic);
            List<QueueData> queueDatas = topicRouteData.getQueueDatas();
            List<BrokerData> brokerDatas = topicRouteData.getBrokerDatas();


            HashMap<String, String> brokerName2MaterBrokerAddr = new HashMap<>();
            for (BrokerData brokerData : brokerDatas) {
                String masterBrokerAddr = brokerData.getBrokerAddrs().get(0L);
                brokerName2MaterBrokerAddr.put(brokerData.getBrokerName(), masterBrokerAddr);
            }

            for (QueueData queueData : queueDatas) {
                int readQueueNums = queueData.getReadQueueNums();
                int writeQueueNums = queueData.getWriteQueueNums();
                String brokerName = queueData.getBrokerName();

                TopicConfig topicConfig = new TopicConfig(stateTopic, readQueueNums, writeQueueNums);

                HashMap<String, String> temp = new HashMap<>();
                temp.put("+delete.policy", "COMPACTION");
                topicConfig.setAttributes(temp);

                mqAdmin.createAndUpdateTopicConfig(brokerName2MaterBrokerAddr.get(brokerName), topicConfig);
            }

            existTopic.add(stateTopic);
        } catch (Throwable t) {
            throw new RuntimeException("create state topic error.", t);
        }

    }

    @Override
    public void close() throws Exception {

    }
}
