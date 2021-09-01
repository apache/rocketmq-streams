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

package org.apache.rocketmq.streams;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportOffsetResetSource;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.debug.DebugWriter;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;

public class RocketMQOffset implements OffsetStore {
    protected OffsetStore offsetStore;
    protected AbstractSupportOffsetResetSource source;
    private AtomicBoolean starting;

    public RocketMQOffset(OffsetStore offsetStore, AbstractSupportOffsetResetSource source){
        this.offsetStore=offsetStore;
        this.source=source;
        this.starting = new AtomicBoolean(true);
    }
    @Override
    public void load() throws MQClientException {
        offsetStore.load();
    }

    @Override
    public void updateOffset(MessageQueue mq, long offset, boolean increaseOnly) {
        offsetStore.updateOffset(mq,offset,increaseOnly);
    }

    @Override
    public long readOffset(MessageQueue mq, ReadOffsetType type) {
         return offsetStore.readOffset(mq,type);
    }

    @Override
    public void persistAll(Set<MessageQueue> mqs) {
        offsetStore.persistAll(mqs);
    }

    @Override
    public void persist(MessageQueue mq) {
        offsetStore.persist(mq);
    }

    @Override
    public void removeOffset(MessageQueue mq) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(new RocketMQMessageQueue(mq).getQueueId());

        //todo 启动时第一次做rebalance时source中也没有原有消费mq，不做移除，做了会有副作用
        //后续整个checkpoint机制都会调整成异步，整块代码都不会保留，目前为了整体跑通，不做修改。
        if (!starting.get()) {
            source.removeSplit(splitIds);
            starting.set(false);
        }

        offsetStore.removeOffset(mq);
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        return offsetStore.cloneOffsetTable(topic);
    }

    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        source.sendCheckpoint(new RocketMQMessageQueue(mq).getQueueId());
        if(DebugWriter.isOpenDebug()){
            ConcurrentMap<MessageQueue, AtomicLong>offsetTable=ReflectUtil.getDeclaredField(this.offsetStore,"offsetTable");
            DebugWriter.getInstance(source.getTopic()).writeSaveOffset(mq,offsetTable.get(mq));
        }
       offsetStore.updateConsumeOffsetToBroker(mq,offset,isOneway);
    }
}
