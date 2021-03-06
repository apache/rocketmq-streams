package org.apache.rocketmq.streams.source;
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
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

public class MessageListenerDelegator implements MessageQueueListener {
    private final MessageQueueListener delegator;
    private final Set<MessageQueue> lastDivided = new HashSet<>();
    private final Set<MessageQueue> removingQueue = new HashSet<>();
    private final AtomicBoolean needSync = new AtomicBoolean(false);
    private final Object mutex = new Object();

    public MessageListenerDelegator(MessageQueueListener delegator) {
        this.delegator = delegator;
    }


    @Override
    public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
        //上一次分配有，但是这一次没有,需要对这些mq进行状态移除
        for (MessageQueue last : lastDivided) {
            if (!mqDivided.contains(last)) {
                removingQueue.add(last);
            }
        }

        this.lastDivided.clear();
        this.lastDivided.addAll(mqDivided);

        needSync.set(true);
        delegator.messageQueueChanged(topic, mqAll, mqDivided);

        synchronized (this.mutex) {
            this.mutex.notifyAll();
        }
    }

    public Set<MessageQueue> getLastDivided() {
        return Collections.unmodifiableSet(this.lastDivided);
    }

    public Set<MessageQueue> getRemovingQueue() {
        return Collections.unmodifiableSet(this.removingQueue);
    }


    public boolean needSync() {
        return this.needSync.get();
    }

    public void hasSynchronized() {
        this.needSync.set(false);
    }

    public Object getMutex() {
        return mutex;
    }
}
