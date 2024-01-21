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
package org.apache.rocketmq.streams.window.fire;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * 对齐等待，当所有的数据源分片的数据都收到，才会返回事件时间
 */
public class SplitEventTimeManager {
    private static Long splitReadyTime;
    private final AtomicInteger queueIdCount = new AtomicInteger(0);
    protected Map<String, Long> messageSplitId2MaxTime = new HashMap<>();
    protected Long lastUpdateTime;
    protected volatile Integer allSplitSize;
    protected Map<String, List<ISplit<?, ?>>> splitsGroupByInstance;
    protected ISource<?> source;
    protected volatile boolean isAllSplitReceived = false;
    protected transient String queueId;

    public SplitEventTimeManager(ISource<?> source, String queueId) {
        this.source = source;
        this.queueId = queueId;
        AbstractSource abstractSource = (AbstractSource) source;
        List<ISplit<?, ?>> splits = abstractSource.fetchAllSplits();
        if (splits == null) {
            this.allSplitSize = -1;
        } else {
            this.allSplitSize = splits.size();
        }
    }

    /**
     * 更新数据源分片的事件时间，取当前收到消息的最大值
     *
     * @param message
     * @param window
     */
    public void updateEventTime(IMessage message, AbstractWindow window) {
        String oriQueueId = message.getMessageBody().getString(WindowConstants.ORIGIN_QUEUE_ID);
        if (StringUtil.isEmpty(oriQueueId)) {
            return;
        }
        Long occurTime = WindowInstance.getOccurTime(window, message);
        Long oldTime = messageSplitId2MaxTime.get(oriQueueId);
        if (oldTime == null) {
            queueIdCount.incrementAndGet();
            messageSplitId2MaxTime.put(oriQueueId, occurTime);
        } else {
            if (occurTime > oldTime) {
                messageSplitId2MaxTime.put(oriQueueId, occurTime);
            }
        }
    }

    /**
     * 获取事件时间，对齐等待，期望获取所有分片的数据后开始，如果一直没有数据，则等1分钟后开始
     * 对齐等待：先判断是否所有的数据源分片的数据都收到过，这块是通过数据源接口获取所有分片，然后根据收到信息的分片做对比，这种实现有两个风险：
     * 1.数据分片不均匀，会导致某些分片的数据一直收不到，导致窗口一直未能触发，尤其是数据在分片中按shuffle key存储
     * 2.要求所有的数据源必须提供获取所有分片的接口，不友好
     * 最新实现：
     * 1.所有数据源启动后或在窗口shuffle前，收到第一条消息后，发一个系统消息
     * 2.通过系统消息，确保分片已经正常工作，且能和数据源的时间快速对齐
     *
     * @return
     */
    public Long getMaxEventTime() {

        if (!isSplitsReceiver()) {
            return null;
        }
        Long min = null;
        Set<Long> eventTimes = new HashSet<>(messageSplitId2MaxTime.values());
        for (Long eventTime : eventTimes) {
            if (eventTime == null) {
                return null;
            }
            if (min == null) {
                min = eventTime;
            } else {
                if (eventTime < min) {
                    min = eventTime;
                }
            }
        }
        return min;

    }

    protected boolean isSplitsReceiver() {
        if (isAllSplitReceived) {
            return true;
        }
        if (lastUpdateTime == null) {
            lastUpdateTime = System.currentTimeMillis();
        }
        int workingSplitSize = this.messageSplitId2MaxTime.size();
        if (allSplitSize == null) {
            if (source == null) {
                return false;
            }
            AbstractSource abstractSource = (AbstractSource) source;
            List<ISplit<?, ?>> splits = abstractSource.fetchAllSplits();
            if (splits == null) {
                this.allSplitSize = -1;
            } else {
                this.allSplitSize = splits.size();
            }
        }
        if (allSplitSize == -1) {
            return true;
        }

        if (allSplitSize != -1 && allSplitSize > workingSplitSize) {
            if (System.currentTimeMillis() - lastUpdateTime > 1000) {
                workingSplitSize = this.messageSplitId2MaxTime.size();
                lastUpdateTime = System.currentTimeMillis();
                if (allSplitSize > workingSplitSize) {
                    return false;
                }
            }
            if (this.splitsGroupByInstance == null) {
                return false;
            }
            //add time out policy: no necessary waiting for other split
            if (splitReadyTime == null) {
                synchronized (this) {
                    if (splitReadyTime == null) {
                        splitReadyTime = System.currentTimeMillis();
                    }
                }
            }
            if (System.currentTimeMillis() - splitReadyTime >= 1000 * 60) {
                this.isAllSplitReceived = true;
                return true;
            }
        }

        if (workingSplitSize == messageSplitId2MaxTime.size()) {
            this.isAllSplitReceived = true;
            return true;
        }
        return false;
    }

    public void setSource(ISource source) {
        this.source = source;
    }

}
