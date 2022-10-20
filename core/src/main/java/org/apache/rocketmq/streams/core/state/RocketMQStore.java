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
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

public class RocketMQStore extends AbstractStore {
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final RocksDBStore rocksDBStore;

    protected StoreState state = StoreState.UNINITIALIZED;
    protected final Object lock = new Object();

    private ExecutorService executor = Executors.newFixedThreadPool(5);

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
    public void recover() {

    }

    @Override
    public void loadState(Set<MessageQueue> addQueues) throws Throwable {
        if (addQueues == null || addQueues.size() == 0) {
            return;
        }
        DefaultLitePullConsumer consumer = new DefaultLitePullConsumer(StreamConfig.ROCKETMQ_STREAMS_STATE_CONSUMER_GROUP);
        consumer.start();

        Set<MessageQueue> stateTopicQueue = mappingStateTopicQueue(addQueues);

        consumer.assign(addQueues);
        for (MessageQueue queue : stateTopicQueue) {
            consumer.seekToBegin(queue);
        }

        Future<?> future = this.executor.submit(() -> pullToLast(consumer));
    }

    @Override
    public void removeState(Set<MessageQueue> removeQueues) {
        if (removeQueues == null || removeQueues.size() == 0) {
            return;
        }

        Set<MessageQueue> stateTopicQueue = mappingStateTopicQueue(removeQueues);

    }

    @Override
    public <K, V> V get(K key) {
        return this.rocksDBStore.get(key);
    }

    @Override
    public <K, V> void put(K k, V v) {
        this.rocksDBStore.put(k, v);
    }

    private Set<MessageQueue> mappingStateTopicQueue(Set<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.size() == 0) {
            return new HashSet<>();
        }

        HashSet<MessageQueue> result = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            MessageQueue queue = new MessageQueue(messageQueue.getTopic() + "-stateTopic", messageQueue.getBrokerName(), messageQueue.getQueueId());
            result.add(queue);
        }

        return result;
    }

    private void pullToLast(DefaultLitePullConsumer consumer) {
        List<MessageExt> result = consumer.poll(50);
        while (result !=null && result.size() != 0) {
            replayState(result);
            result = consumer.poll(50);
        }
    }

    private void replayState(List<MessageExt> msgs) {
        if (msgs == null || msgs.size() == 0) {
            return;
        }

        msgs.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
