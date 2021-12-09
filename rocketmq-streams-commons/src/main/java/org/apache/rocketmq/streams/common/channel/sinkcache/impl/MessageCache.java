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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.sinkcache.DataSourceAutoFlushTask;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.schedule.ScheduleManager;
import org.apache.rocketmq.streams.common.schedule.ScheduleTask;

/**
 * 消息缓存的实现，通过消息队列做本地缓存。目前多是用了这个实现
 */
public class MessageCache<R> implements IMessageCache<R> {

    protected IMessageFlushCallBack<R> flushCallBack;
    protected volatile AtomicInteger messageCount = new AtomicInteger(0);//缓存中的数据条数
    protected int batchSize = 1000;//最大缓存条数，超过后需要，刷新出去，做内存保护
    protected transient DataSourceAutoFlushTask autoFlushTask;//自动任务刷新，可以均衡实时性和吞吐率
    protected volatile transient ConcurrentLinkedQueue<R> dataQueue = new ConcurrentLinkedQueue<>();//缓存数据的消息队列
    protected AtomicBoolean openAutoFlushLock = new AtomicBoolean(false);
    protected volatile int autoFlushSize = 300;
    protected volatile int autoFlushTimeGap = 1000;

    protected ExecutorService autoFlushExecutorService;


    public MessageCache(IMessageFlushCallBack<R> flushCallBack) {
        this.flushCallBack = flushCallBack;
    }

    /**
     * 把待插入的数据写入队列 如果缓存超过batchsize，需要强制刷新
     *
     * @param msg
     * @return
     */
    @Override
    public int addCache(R msg) {
        offerQueue(msg);
        int size = messageCount.incrementAndGet();
        if (batchSize > 0 && size >= batchSize) {
            flush();
        }
        return size;
    }

    @Override
    public void openAutoFlush() {
        if (openAutoFlushLock.compareAndSet(false, true)) {//可重入锁
            autoFlushTask = new DataSourceAutoFlushTask(true, this);
            autoFlushTask.setAutoFlushSize(this.autoFlushSize);
            autoFlushTask.setAutoFlushTimeGap(this.autoFlushTimeGap);
            ScheduleTask scheduleTask=new ScheduleTask(autoFlushTask,autoFlushTask);
            scheduleTask.setExecutorService(this.autoFlushExecutorService);
            ScheduleManager.getInstance().regist(scheduleTask);
        }
    }

    @Override
    public void closeAutoFlush() {
        if (autoFlushTask != null) {
            autoFlushTask.setAutoFlush(false);
            openAutoFlushLock.set(false);
        }
    }

    protected  void offerQueue(R msg) {
        dataQueue.offer(msg);

    }

    protected List<R> getMessagesFromQueue(int size) {
        List<R> messages = new ArrayList<>();
        int count=0;
        while (count<size) {
            R msg = this.dataQueue.poll();
            messages.add(msg);
            count++;
        }
        return messages;
    }

    @Override
    public Integer getMessageCount() {
        return messageCount.get();
    }

    /**
     * 把队列排空，并写入到存储中
     *
     * @return
     */
    @Override
    public int flush() {
        if (getMessageCount() == 0) {
            return 0;
        }
        List<R> messages = null;
        synchronized (this) {
            if (getMessageCount() == 0) {
                return 0;
            }
            int size=this.dataQueue.size();
            messageCount = new AtomicInteger(0);
            messages = getMessagesFromQueue(size);
            flushCallBack.flushMessage(messages);
            return messages.size();
        }

    }

    @Override
    public int flush(Set<String> splitIds) {
        return flush();
    }


    public int getAutoFlushSize() {
        return autoFlushSize;
    }

    public void setAutoFlushSize(int autoFlushSize) {
        this.autoFlushSize = autoFlushSize;
    }

    public int getAutoFlushTimeGap() {
        return autoFlushTimeGap;
    }

    public void setAutoFlushTimeGap(int autoFlushTimeGap) {
        this.autoFlushTimeGap = autoFlushTimeGap;
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public IMessageFlushCallBack<R> getFlushCallBack() {
        return flushCallBack;
    }

    public ExecutorService getAutoFlushExecutorService() {
        return autoFlushExecutorService;
    }

    public void setAutoFlushExecutorService(ExecutorService autoFlushExecutorService) {
        this.autoFlushExecutorService = autoFlushExecutorService;
    }
}
