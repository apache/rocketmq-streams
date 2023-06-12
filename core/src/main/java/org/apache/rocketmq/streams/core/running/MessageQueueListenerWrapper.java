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

import com.google.common.collect.Sets;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

class MessageQueueListenerWrapper implements MessageQueueListener {
    private static final Logger logger = LoggerFactory.getLogger(MessageQueueListenerWrapper.class.getName());
    private final MessageQueueListener originListener;
    private final TopologyBuilder topologyBuilder;

    private final ConcurrentHashMap<String, Set<MessageQueue>> ownedMapping = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Processor<?>> mq2Processor = new ConcurrentHashMap<>();

    private BiFunction<Set<MessageQueue>, Set<MessageQueue>, Throwable> recoverHandler;

    MessageQueueListenerWrapper(MessageQueueListener originListener, TopologyBuilder topologyBuilder) {
        this.originListener = originListener;
        this.topologyBuilder = topologyBuilder;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        Set<MessageQueue> ownedQueues = ownedMapping.computeIfAbsent(topic, s -> new HashSet<>());

        Set<MessageQueue> unchangedQueue = Sets.intersection(mqDivided, ownedQueues);
        Set<MessageQueue> addQueue = Sets.difference(mqDivided, unchangedQueue);
        Set<MessageQueue> removeQueue = Sets.difference(ownedQueues, unchangedQueue);

        ownedQueues.addAll(new HashSet<>(addQueue));
        ownedQueues.removeAll(new HashSet<>(removeQueue));

        // First step, remove the removeQueue from listener to avoid inflight data in between setting up the state.
        originListener.messageQueueChanged(topic, mqAll, unchangedQueue);

        //从shuffle topic中读出的数据才能进行有状态计算。
        if (topic.endsWith(Constant.SHUFFLE_TOPIC_SUFFIX)) {
            Throwable throwable = this.recoverHandler.apply(addQueue, removeQueue);
            if (throwable != null) {
                throw new RuntimeException(throwable);
            }
            logger.info("recover messageQueue finish, addQueue: [{}], removeQueue:[{}].", addQueue, removeQueue);
        }

        buildTask(addQueue);
        removeTask(removeQueue);
        // Last step, add the addQueue to the listener after the state setup.
        originListener.messageQueueChanged(topic, mqAll, mqDivided);
    }


    private void buildTask(Set<MessageQueue> addQueues) {
        for (MessageQueue messageQueue : addQueues) {
            String key = Utils.buildKey(messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
            if (!mq2Processor.containsKey(key)) {
                Processor<?> processor = topologyBuilder.build(messageQueue.getTopic());
                this.mq2Processor.put(key, processor);
            }
        }
    }

    private void removeTask(Set<MessageQueue> removeQueues) {
        for (MessageQueue removeQueue : removeQueues) {
            String key = Utils.buildKey(removeQueue.getBrokerName(), removeQueue.getTopic(), removeQueue.getQueueId());
            mq2Processor.remove(key);
        }
    }

    @SuppressWarnings("unchecked")
    <T> Processor<T> selectProcessor(String key) {
        return (Processor<T>) this.mq2Processor.get(key);
    }

    public void setRecoverHandler(BiFunction<Set<MessageQueue>, Set<MessageQueue>, Throwable> handler) {
        this.recoverHandler = handler;
    }
}
