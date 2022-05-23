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
package org.apache.rocketmq.streams.common.channel.impl.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.IMessage;

public class MemorySink extends AbstractSupportShuffleSink implements IAfterConfigurableRefreshListener {
    /**
     * 是否启动qps的统计
     */
    protected transient volatile boolean startQPSCount = false;
    /**
     * 总处理数据数
     */
    protected transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 最早的处理时间
     */
    protected transient long firstReceiveTime = System.currentTimeMillis();
    protected String cacheName;
    protected transient MemoryCache memoryCache;

    public MemorySink() {
    }

    @Override
    protected boolean batchInsert(List<IMessage> messages) {
        if (startQPSCount) {
            long count = COUNT.addAndGet(messages.size());
            long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
            double qps = count / second;
            System.out.println("qps is " + qps + "。the count is " + count + ".the process time is " + second);
        }
        try {
            for (IMessage msg : messages) {
                memoryCache.queue.offer(msg.getMessageValue());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public String getShuffleTopicFieldName() {
        return null;
    }

    @Override
    protected void createTopicIfNotExist(int splitNum) {

    }

    @Override
    public List<ISplit<?,?>> getSplitList() {
        List<ISplit<?,?>> splits = new ArrayList<>();
        splits.add(new MemorySplit());
        return splits;
    }

    @Override
    public int getSplitNum() {
        return 1;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        memoryCache = configurableService.queryConfigurable(MemoryCache.TYPE, cacheName);
    }

    public String getCacheName() {
        return cacheName;
    }

    public void setCacheName(String cacheName) {
        this.cacheName = cacheName;
    }

    public void setMemoryCache(MemoryCache memoryCache) {
        this.memoryCache = memoryCache;
        setCacheName(memoryCache.getConfigureName());

    }
}
