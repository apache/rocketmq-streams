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
package org.apache.rocketmq.streams.metaq;

import com.alibaba.rocketmq.client.consumer.store.OffsetStore;
import com.alibaba.rocketmq.client.consumer.store.ReadOffsetType;
import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageQueue;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.metaq.debug.DebugWriter;
import org.apache.rocketmq.streams.metaq.queue.MetaqMessageQueue;
import org.apache.rocketmq.streams.metaq.source.MetaqSource;

public class MetaqOffset implements OffsetStore {
    protected OffsetStore offsetStore;
    protected MetaqSource metaqSource;
    protected transient volatile Long time = null;
    protected transient Long time1 = null;

    public MetaqOffset(OffsetStore offsetStore, MetaqSource metaqSource) {
        this.offsetStore = offsetStore;
        this.metaqSource = metaqSource;
    }

    @Override
    public void load() throws MQClientException {
        offsetStore.load();
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        offsetStore.updateOffset(mq, offset, increaseOnly);
    }

    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType type) {
        return offsetStore.readOffset(mq, type);
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        if (mqs.size() == 0) {
            return;
        }
        Set<String> queueIds = new HashSet<>();
        for (MessageQueue mq : mqs) {
            MetaqMessageQueue metaqMessageQueue = new MetaqMessageQueue(mq);
            queueIds.add(metaqMessageQueue.getQueueId());
        }
        //ConcurrentHashMap<MessageQueue, AtomicLong> offsetTable=ReflectUtil.getDeclaredField(offsetStore,"offsetTable");
        //metaqSource.fileOffset.save(offsetTable);
        metaqSource.sendCheckpoint(queueIds);
        if (DebugWriter.isOpenDebug()) {
            ConcurrentMap<MessageQueue, AtomicLong> offsetTable = ReflectUtil.getDeclaredField(this.offsetStore, "offsetTable");
            DebugWriter.getInstance(metaqSource.getTopic()).writeSaveOffset(offsetTable);
        }
        offsetStore.persistAll(mqs);
    }

    @Override
    public void persist(MessageQueue mq) {
        MetaqMessageQueue metaqMessageQueue = new MetaqMessageQueue(mq);
        metaqSource.sendCheckpoint(metaqMessageQueue.getQueueId());
        if (DebugWriter.isOpenDebug()) {
            ConcurrentMap<org.apache.rocketmq.common.message.MessageQueue, AtomicLong> offsetTable = ReflectUtil.getDeclaredField(this.offsetStore, "offsetTable");
            DebugWriter.getInstance(metaqMessageQueue.getTopic()).writeSaveOffset(mq, offsetTable.get(mq));
        }
        offsetStore.persist(mq);
    }

    @Override
    public void removeOffset(MessageQueue mq) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(new MetaqMessageQueue(mq).getQueueId());
        metaqSource.removeSplit(splitIds);
        offsetStore.removeOffset(mq);
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        return offsetStore.cloneOffsetTable(topic);
    }

    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway)
        throws MQBrokerException, InterruptedException, MQClientException, com.alibaba.rocketmq.remoting.exception.RemotingException {
        offsetStore.updateConsumeOffsetToBroker(mq, offset, isOneway);
    }
}
