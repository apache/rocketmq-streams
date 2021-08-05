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
package org.apache.rocketmq.streams.common.channel.sinkcache.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.context.IMessage;

public abstract class AbstractMutilSplitMessageCache<R> extends MessageCache<R> {
    protected ConcurrentHashMap<String, MessageCache<IMessage>> queueMessageCaches = new ConcurrentHashMap();

    public AbstractMutilSplitMessageCache(
        IMessageFlushCallBack<R> flushCallBack) {
        super(null);
        this.flushCallBack = new MessageFlushCallBack(flushCallBack);
    }

    @Override
    public int addCache(R msg) {
        String queueId = createSplitId(msg);
        MessageCache messageCache = new MessageCache(flushCallBack);
        MessageCache existMessageCache = queueMessageCaches.putIfAbsent(queueId, messageCache);
        if (existMessageCache != null) {
            messageCache = existMessageCache;
        } else {
            messageCache.setBatchSize(batchSize);
            messageCache.openAutoFlush();
        }
        messageCache.addCache(msg);
        int size = messageCount.incrementAndGet();
        if (batchSize > 0 && size >= batchSize) {
            flush(queueId);
            size = messageCount.get();
        }
        return size;
    }

    protected abstract String createSplitId(R msg);

    @Override
    public int flush() {
        int size = 0;
        for (IMessageCache cache : this.queueMessageCaches.values()) {
            size += cache.flush();
        }
        return size;
    }

    @Override
    public int flush(Set<String> splitIds) {
        int size=0;
        if(queueMessageCaches==null||queueMessageCaches.size()==0){
            return 0;
        }
        for(String splitId:splitIds){

            IMessageCache cache=  queueMessageCaches.get(splitId);
            if(cache!=null){
                size+=cache.flush();
            }

        }
        return size;
    }

    protected int flush(String splitId) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(splitId);
        return flush(splitIds);
    }

    @Override
    public Integer getMessageCount() {
        return messageCount.get();
    }

    @Override
    public void openAutoFlush() {
        if (this.queueMessageCaches == null) {
            return;
        }
        for (IMessageCache cache : this.queueMessageCaches.values()) {
            cache.openAutoFlush();
        }
    }

    @Override
    public void closeAutoFlush() {
        for (IMessageCache cache : this.queueMessageCaches.values()) {
            cache.closeAutoFlush();
        }
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    protected class MessageFlushCallBack implements IMessageFlushCallBack<R> {
        protected IMessageFlushCallBack<R> callBack;

        public MessageFlushCallBack(IMessageFlushCallBack<R> callBack) {
            this.callBack = callBack;
        }

        @Override
        public boolean flushMessage(List<R> messages) {
            boolean success = callBack.flushMessage(messages);
            messageCount.addAndGet(-messages.size());
            return success;
        }
    }
}
