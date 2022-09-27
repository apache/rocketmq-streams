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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class SplitEventTimeManager {
    protected static final Log LOG = LogFactory.getLog(SplitEventTimeManager.class);
    protected static final Map<String, Long> messageSplitId2MaxTime = new HashMap<>();
    private AtomicInteger queueIdCount = new AtomicInteger(0);

    protected volatile Integer allSplitSize;
    protected ISource source;
    protected transient String queueId;

    public SplitEventTimeManager(ISource source, String queueId) {
        this.source = source;
        this.queueId = queueId;
        if (source instanceof AbstractSource) {
            AbstractSource abstractSource = (AbstractSource) source;
            List<ISplit> splits = abstractSource.getAllSplits();
            if (splits == null) {
                this.allSplitSize = -1;
            } else {
                this.allSplitSize = splits.size();
            }
        } else {
            this.allSplitSize = -1;
        }
    }

    public void updateEventTime(IMessage message, AbstractWindow window) {
        String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
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

    public Long getMaxEventTime() {
        Long min = null;

        synchronized (messageSplitId2MaxTime) {
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
        }

        return min;

    }



    public void setSource(ISource source) {
        this.source = source;
    }
}
