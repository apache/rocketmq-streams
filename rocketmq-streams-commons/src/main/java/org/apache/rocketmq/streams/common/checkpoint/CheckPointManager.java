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
package org.apache.rocketmq.streams.common.checkpoint;

import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class CheckPointManager {
    protected IMessageCache<CheckPointMessage> messageCache;
    protected transient Map<String, Long> currentSplitAndLastUpdateTime = new HashMap<>();//保存这个实例处理的分片数

    protected transient Map<String, Long> removingSplits = new HashMap<>();//正在删除的分片

    public CheckPointManager() {
        messageCache = new MessageCache<>(new IMessageFlushCallBack<CheckPointMessage>() {
            @Override
            public boolean flushMessage(List<CheckPointMessage> messages) {
                //合并最近的checkpoint，只存储一次，为了
                Map<String, SourceState> sourceStateMap = mergeSourceState(messages);
                saveCheckPoint(sourceStateMap);
                return true;
            }
        });
        messageCache.openAutoFlush();
    }

    public void flush() {
        messageCache.flush();
    }

    public synchronized void addSplit(String splitId) {
        this.currentSplitAndLastUpdateTime.put(splitId, System.currentTimeMillis());
    }

    public synchronized void removeSplit(String splitId) {
        this.currentSplitAndLastUpdateTime.remove(splitId);
    }

    public boolean contains(String splitId) {
        return this.currentSplitAndLastUpdateTime.containsKey(splitId);
    }

    /**
     * 可能有多次的offset合并在一起，对offset合并 合并包含两个逻辑：1.同1个CheckPointMessage中，选择最小的作为本次的offset
     *
     * @param messages
     */
    protected Map<String, SourceState> mergeSourceState(List<CheckPointMessage> messages) {
        Map<String, SourceState> sourceStateMap = new HashMap<>();
        for (CheckPointMessage checkPointMessage : messages) {
            SourceState sourceState = createSourceState(checkPointMessage);
            if (sourceState == null) {
                continue;
            }
            String sourceName = sourceState.getSourceName();
            SourceState existSourceState = sourceStateMap.get(sourceName);
            if (existSourceState != null) {
                SourceState lastSourceState = merge(sourceState, existSourceState);
                sourceStateMap.put(sourceName, lastSourceState);
            }
        }
        return sourceStateMap;
    }

    /**
     * 一个pipeline流程中，找最小的offset提交保存
     *
     * @param checkPointMessage
     * @return
     */
    protected SourceState createSourceState(CheckPointMessage checkPointMessage) {
        SourceState sourceState = new SourceState();
        String pipelineName = null;
        if (checkPointMessage.getStreamOperator() instanceof IConfigurableIdentification) {
            IConfigurableIdentification configurable = (IConfigurableIdentification)checkPointMessage.getCheckPointStates();
            pipelineName = configurable.getConfigureName();
        }
        Map<String, MessageOffset> queueId2Offsets = new HashMap<>();
        sourceState.setSourceName(createSourceName(checkPointMessage.getSource(), pipelineName));
        sourceState.setQueueId2Offsets(queueId2Offsets);

        for (CheckPointState checkPointState : checkPointMessage.getCheckPointStates()) {
            if (checkPointState.isReplyAnyOny()) {
                continue;
            }
            if (checkPointState.isReplyRefuse()) {
                return null;
            }
            for (Entry<String, MessageOffset> entry : checkPointState.getQueueIdAndOffset().entrySet()) {
                String queueId = entry.getKey();
                MessageOffset offset = entry.getValue();
                MessageOffset existOffset = queueId2Offsets.get(queueId);
                if (existOffset == null) {
                    queueId2Offsets.put(queueId, offset);
                } else {
                    boolean isGreateThan = existOffset.greateThan(offset.getOffsetStr());
                    if (isGreateThan) {
                        queueId2Offsets.put(queueId, offset);
                    } else {
                        queueId2Offsets.put(queueId, existOffset);
                    }
                }
            }
        }
        return sourceState;
    }

    /**
     * 先查询现在数据源的分片，如果已经不处理的分片，不做保存 否则把结果保存到db中
     *
     * @param sourceStateMap
     */
    protected void saveCheckPoint(Map<String, SourceState> sourceStateMap) {

    }

    /**
     * 如果多次的checkpoint在一起，先合并再保存
     *
     * @param sourceState
     * @param existSourceState
     * @return
     */
    protected SourceState merge(SourceState sourceState, SourceState existSourceState) {
        Iterator<Entry<String, MessageOffset>> it = sourceState.getQueueId2Offsets().entrySet()
            .iterator();
        while (it.hasNext()) {
            Entry<String, MessageOffset> entry = it.next();
            String queueId = entry.getKey();
            MessageOffset offset = entry.getValue();
            MessageOffset existOffset = existSourceState.getQueueId2Offsets().get(queueId);
            if (existOffset == null) {
                existSourceState.getQueueId2Offsets().put(queueId, offset);
            } else {
                boolean isGreaterThan = offset.greateThan(existOffset.getOffsetStr());
                if (isGreaterThan) {
                    existSourceState.getQueueId2Offsets().put(queueId, offset);
                }
            }
        }
        return existSourceState;
    }

    public void addCheckPointMessage(CheckPointMessage message) {
        this.messageCache.addCache(message);
    }



    public void updateLastUpdate(String queueId) {
        addSplit(queueId);
    }

    public Set<String> getCurrentSplits() {
        return this.currentSplitAndLastUpdateTime.keySet();
    }

    public static class SourceState {
        protected String sourceName;
        protected Map<String, MessageOffset> queueId2Offsets = new HashMap<>();

        public String getSourceName() {
            return sourceName;
        }

        public void setSourceName(String sourceName) {
            this.sourceName = sourceName;
        }

        public Map<String, MessageOffset> getQueueId2Offsets() {
            return queueId2Offsets;
        }

        public void setQueueId2Offsets(
            Map<String, MessageOffset> queueId2Offsets) {
            this.queueId2Offsets = queueId2Offsets;
        }
    }

    /**
     * 根据source进行划分，主要是针对双流join的场景
     *
     * @param source
     * @return
     */
    public static String createSourceName(ISource source, String piplineName) {
        String namespace = null;
        String name = null;
        if (source != null) {
            namespace = source.getNameSpace();
            name = source.getConfigureName();
        }

        if (StringUtil.isEmpty(namespace)) {
            namespace = "default_namespace";
        }
        if (StringUtil.isEmpty(name)) {
            name = "default_name";
        }
        if (StringUtil.isEmpty(piplineName)) {
            piplineName = "default_piplineName";
        }
        return MapKeyUtil.createKey(namespace, piplineName, name);
    }

    public Map<String, Long> getCurrentSplitAndLastUpdateTime() {
        return currentSplitAndLastUpdateTime;
    }

    public synchronized void addRemovingSplit(Set<String> removingSplits) {
        long removingTime = System.currentTimeMillis();
        for (String split : removingSplits) {
            this.removingSplits.put(split, removingTime);
        }
    }

    public synchronized void deleteRemovingSplit(Set<String> removingSplits) {
        for (String split : removingSplits) {
            this.removingSplits.remove(split);
        }

    }

    public synchronized boolean isRemovingSplit(String splitId) {
        Long removingTime = this.removingSplits.get(splitId);
        if (removingTime == null) {
            return false;
        }
        //超过10秒才允许当作新分片进来
        if (System.currentTimeMillis() - removingTime > 10 * 1000) {
            this.removingSplits.remove(splitId);
            return false;
        }
        return true;
    }
}
