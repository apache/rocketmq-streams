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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class AbstractMultiSplitMessageCache<R> extends MessageCache<R> {
    protected ConcurrentHashMap<String, MessageCache<IMessage>> queueMessageCaches = new ConcurrentHashMap();
    protected transient Boolean isOpenAutoFlush = true;
    protected transient ExecutorService executorService;

    public AbstractMultiSplitMessageCache(
        IMessageFlushCallBack<R> flushCallBack) {
        super(null);
//        this.executorService = new ThreadPoolExecutor(10, 10,
//            0L, TimeUnit.MILLISECONDS,
//            new LinkedBlockingQueue<Runnable>(), new ThreadPoolFactory.DipperThreadFactory("AbstractMultiSplitMessageCache"));
        this.executorService = ThreadPoolFactory.createFixedThreadPool(10, AbstractMultiSplitMessageCache.class.getName() + "-message_cache");
        this.flushCallBack = new MessageFlushCallBack(flushCallBack);
    }

    @Override
    public int addCache(R msg) {
        String queueId = createSplitId(msg);
        MessageCache messageCache = queueMessageCaches.get(queueId);
        if (messageCache == null) {
            synchronized (this) {
                messageCache = queueMessageCaches.get(queueId);
                if (messageCache == null) {
                    messageCache = createMessageCache();
                    messageCache.setAutoFlushSize(this.autoFlushSize);
                    messageCache.setAutoFlushTimeGap(this.autoFlushTimeGap);
                    messageCache.setBatchSize(batchSize);
                    if (this.isOpenAutoFlush) {
                        messageCache.openAutoFlush();
                    }
                    MessageCache existMessageCache = queueMessageCaches.putIfAbsent(queueId, messageCache);
                    if (existMessageCache != null) {
                        messageCache = existMessageCache;
                    }
                }
            }

        }
        messageCache.addCache(msg);
        int size = messageCount.incrementAndGet();
        if (batchSize > 0 && size >= batchSize) {
            flush(queueId);
            size = messageCount.get();
        }
        return size;
    }

    protected MessageCache createMessageCache() {
        return new MessageCache(flushCallBack);
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
        AtomicInteger size = new AtomicInteger(0);
        if (queueMessageCaches == null || queueMessageCaches.size() == 0) {
            return 0;
        }
        if (splitIds == null || splitIds.size() == 0) {
            return 0;
        }
        if (splitIds.size() == 1) {
            String spiltId = splitIds.iterator().next();
            if (StringUtil.isEmpty(spiltId)) {
                return 0;
            }
            IMessageCache cache = queueMessageCaches.get(spiltId);
            if (cache == null) {
                return 0;
            }
            int count = cache.flush();
            size.addAndGet(count);
            return size.get();
        }
        CountDownLatch countDownLatch = new CountDownLatch(splitIds.size());
        for (String splitId : splitIds) {
            if (StringUtil.isEmpty(splitId)) {
                continue;
            }
            executorService.execute(new Runnable() {
                @Override public void run() {
                    IMessageCache cache = queueMessageCaches.get(splitId);
                    if (cache != null) {
                        int count = cache.flush();
                        size.addAndGet(count);

                    }
                    countDownLatch.countDown();
                }
            });

        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return size.get();
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
        for (MessageCache cache : this.queueMessageCaches.values()) {
            cache.setAutoFlushSize(this.autoFlushSize);
            cache.setAutoFlushTimeGap(this.autoFlushTimeGap);
            cache.openAutoFlush();
        }
        this.isOpenAutoFlush = true;
    }

    @Override
    public void closeAutoFlush() {
        this.isOpenAutoFlush = false;
        for (IMessageCache cache : this.queueMessageCaches.values()) {
            cache.closeAutoFlush();
        }
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
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
