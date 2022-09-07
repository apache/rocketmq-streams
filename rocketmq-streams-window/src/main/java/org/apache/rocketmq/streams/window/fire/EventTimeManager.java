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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

public class EventTimeManager {
    private Map<String, SplitEventTimeManager> eventTimeManagerMap = new HashMap<>();
    protected ISource source;

    private Map<String, Pair<Long, Long>> eventTimeIncreasementMap = new ConcurrentHashMap<>();

    public void updateEventTime(IMessage message, AbstractWindow window) {
        String queueId = message.getHeader().getQueueId();
        SplitEventTimeManager splitEventTimeManager = eventTimeManagerMap.get(queueId);
        if (splitEventTimeManager == null) {
            synchronized (this) {
                splitEventTimeManager = eventTimeManagerMap.get(queueId);
                if (splitEventTimeManager == null) {
                    splitEventTimeManager = new SplitEventTimeManager(source, queueId);
                    eventTimeManagerMap.put(queueId, splitEventTimeManager);
                }
            }
        }
        splitEventTimeManager.updateEventTime(message, window);
    }

    public Long getMaxEventTime(String queueId) {
        SplitEventTimeManager splitEventTimeManager = eventTimeManagerMap.get(queueId);
        if (splitEventTimeManager != null) {
            Long currentMaxEventTime = splitEventTimeManager.getMaxEventTime();
            if (currentMaxEventTime == null) {
                return null;
            }
            if (eventTimeIncreasementMap.containsKey(queueId)) {
                Long lastMaxEventTime = eventTimeIncreasementMap.get(queueId).getKey();
                if (lastMaxEventTime != null && lastMaxEventTime.equals(currentMaxEventTime)) {
                    //increase event time as time flies to solve batch data processing issue
                    if (System.currentTimeMillis() - eventTimeIncreasementMap.get(queueId).getRight() > IWindow.SYS_DELAY_TIME) {
                        return lastMaxEventTime + (System.currentTimeMillis() - eventTimeIncreasementMap.get(queueId).getRight());
                    }
                } else {
                    eventTimeIncreasementMap.put(queueId, Pair.of(currentMaxEventTime, System.currentTimeMillis()));
                }
            } else {
                eventTimeIncreasementMap.put(queueId, Pair.of(currentMaxEventTime, System.currentTimeMillis()));
            }
            return eventTimeIncreasementMap.get(queueId).getLeft();
        }
        return null;
    }

    public void setSource(ISource source) {
        if (this.source != null) {
            return;
        }
        synchronized (this) {
            this.source = source;
            for (SplitEventTimeManager splitEventTimeManager : eventTimeManagerMap.values()) {
                splitEventTimeManager.setSource(source);
            }
        }

    }
}
