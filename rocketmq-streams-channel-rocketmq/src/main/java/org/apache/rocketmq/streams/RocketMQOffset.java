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
import org.apache.rocketmq.client.consumer.store.OffsetStore;
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.common.channel.source.AbstractSupportOffsetResetSource;
import org.apache.rocketmq.streams.queue.RocketMQMessageQueue;

public class RocketMQOffset implements OffsetStore {
    protected OffsetStore offsetStore;
    protected AbstractSupportOffsetResetSource source;
    public RocketMQOffset(OffsetStore offsetStore, AbstractSupportOffsetResetSource source){
        this.offsetStore=offsetStore;
        this.source=source;
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
        Set<String> queueIds=new HashSet<>();
        for(MessageQueue mq:mqs){
            queueIds.add(new RocketMQMessageQueue(mq).getQueueId());
        }
        source.sendCheckpoint(queueIds);
        offsetStore.persistAll(mqs);
    }

    @Override
    public void persist(MessageQueue mq) {
        source.sendCheckpoint(new RocketMQMessageQueue(mq).getQueueId());
        offsetStore.persist(mq);
    }

    @Override
    public void removeOffset(MessageQueue mq) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(new RocketMQMessageQueue(mq).getQueueId());
        source.removeSplit(splitIds);
        offsetStore.removeOffset(mq);
        offsetStore.removeOffset(mq);
    }

    @Override
    public Map<MessageQueue, Long> cloneOffsetTable(String topic) {
        return offsetStore.cloneOffsetTable(topic);
    }

    @Override
    public void updateConsumeOffsetToBroker(MessageQueue mq, long offset, boolean isOneway)
            throws RemotingException, MQBrokerException, InterruptedException, MQClientException {
        offsetStore.updateOffset(mq,offset,isOneway);
    }
}
