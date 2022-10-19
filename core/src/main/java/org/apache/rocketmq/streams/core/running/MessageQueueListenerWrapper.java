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

import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

class MessageQueueListenerWrapper implements MessageQueueListener {
    private final static InternalLogger log = ClientLogger.getLog();

    private static final String pattern = "%s%s@%s";
    private final MessageQueueListener originListener;
    private final TopologyBuilder topologyBuilder;

    private final ConcurrentHashMap<String, Set<MessageQueue>> ownedMapping = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Processor<?>> mq2Processor = new ConcurrentHashMap<>();

    private Consumer<Set<MessageQueue>> removeQueueHandler;
    private Consumer<Set<MessageQueue>> addQueueHandler;

    MessageQueueListenerWrapper(MessageQueueListener originListener, TopologyBuilder topologyBuilder) {
        this.originListener = originListener;
        this.topologyBuilder = topologyBuilder;
    }

    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        //todo 使用线程池，一个topic一个任务，并发进行，防止某个topic加载状态阻塞后面的topic加载状态；

        Set<MessageQueue> ownedQueues = ownedMapping.get(topic);
        if (ownedQueues == null) {
            ownedQueues = new HashSet<>();
        }

        HashSet<MessageQueue> addQueue = new HashSet<>(mqDivided);
        addQueue.removeAll(ownedQueues);

        HashSet<MessageQueue> removeQueue = new HashSet<>(ownedQueues);
        removeQueue.removeAll(mqDivided);


        CountDownLatch waitPoint = new CountDownLatch(2);

        removeTask(removeQueue);
        ownedQueues.removeAll(removeQueue);
        new Thread(() -> {
            removeQueueHandler.accept(removeQueue);
            waitPoint.countDown();
        }).start();

        buildTask(addQueue);
        ownedQueues.addAll(addQueue);
        new Thread(() -> {
            addQueueHandler.accept(addQueue);
            waitPoint.countDown();
        }).start();

        StringBuilder builder = new StringBuilder();

        for (MessageQueue messageQueue : mqDivided) {
            builder.append(messageQueue.toString());
            builder.append(";");
        }
        String result = builder.substring(0, builder.lastIndexOf(";"));

        long start = System.currentTimeMillis();
        log.info("[start] load and clear state for messageQueue, begin time=[{}], messageQueue=[{}], ", topic, start, result);
        try {
            waitPoint.wait();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            long end = System.currentTimeMillis();
            log.info("[stop] load and clear state for messageQueue, stop time=[{}], consume time=[{}] messageQueue=[{}], ", end, end - start, result);
        }

        //设计的不太好，移除q，添加消费任务之前，应该加一个状态移除函数;目前这样写的问题是：状态提前移除/加载了，consumer其实仍然在从某个将要移除的q中拉取数据，但是状态却被移除了。
        //也不能把originListener.messageQueueChanged放在loadState/removeState之前，那样会已经在拉取数据了，但是状态没有加载好。
        originListener.messageQueueChanged(topic, mqAll, mqDivided);
    }


    private void buildTask(Set<MessageQueue> addQueues) {
        for (MessageQueue messageQueue : addQueues) {
            String key = buildKey(messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
            if (!mq2Processor.containsKey(key)) {
                Processor<?> processor = topologyBuilder.build(messageQueue.getTopic());
                this.mq2Processor.put(key, processor);
            }
        }
    }

    private void removeTask(Set<MessageQueue> removeQueues) {
        for (MessageQueue removeQueue : removeQueues) {
            String key = buildKey(removeQueue.getBrokerName(), removeQueue.getTopic(), removeQueue.getQueueId());
            mq2Processor.remove(key);
        }
    }

    @SuppressWarnings("unchecked")
    <T> Processor<T> selectProcessor(String key) {
        return (Processor<T>) this.mq2Processor.get(key);
    }

    String buildKey(String brokerName, String topic, int queueId) {
        return String.format(pattern, brokerName, topic, queueId);
    }

    public void setRemoveQueueHandler(Consumer<Set<MessageQueue>> removeQueueHandler) {
        this.removeQueueHandler = removeQueueHandler;
    }

    public void setAddQueueHandler(Consumer<Set<MessageQueue>> addQueueHandler) {
        this.addQueueHandler = addQueueHandler;
    }
}
