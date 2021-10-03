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
package org.apache.rocketmq.streams.common.channel.sink;

import com.alibaba.fastjson.JSONObject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MultiSplitMessageCache;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.SourceState;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.interfaces.ILifeCycle;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 输出的接口抽象，针对json消息的场景
 */
public abstract class AbstractSink extends BasedConfigurable implements ISink<AbstractSink>, ILifeCycle {

    private static final Log logger = LogFactory.getLog(AbstractSink.class);
    public static String TARGET_QUEUE = "target_queue";//指定发送queue
    public static final int DEFAULT_BATCH_SIZE = 3000;
    protected transient IMessageCache<IMessage> messageCache;
    protected volatile int batchSize = DEFAULT_BATCH_SIZE;
    protected transient volatile Map<String, SourceState> sourceName2State = new HashMap<>();//保存完成刷新的queueid和offset

    public AbstractSink() {
        setType(TYPE);

    }

    @Override
    protected boolean initConfigurable() {
        messageCache = new MultiSplitMessageCache(this);
        messageCache.openAutoFlush();
        return super.initConfigurable();
    }

    @Override
    public boolean batchAdd(IMessage fieldName2Value, ISplit split) {
        fieldName2Value.getMessageBody().put(TARGET_QUEUE, split);
        return batchAdd(fieldName2Value);
    }

    public ISplit getSplit(IMessage message) {
        return (ISplit) message.getMessageBody().get(TARGET_QUEUE);
    }

    @Override
    public boolean batchAdd(IMessage fieldName2Value) {
        messageCache.addCache(fieldName2Value);
        return true;
    }

    @Override
    public void openAutoFlush() {
        messageCache.openAutoFlush();
    }

    @Override
    public boolean batchSave(List<IMessage> messages) {
        if (messages == null || messages.size() == 0) {
            //LOG.warn("has empty data to insert");
            return true;
        }
        int batchSize = this.batchSize;
        if (batchSize == -1) {
            batchSize = DEFAULT_BATCH_SIZE;
        }
        int length = messages.size();
        if (length <= batchSize) {
            batchInsert(messages);
            return true;
        }
        int count = length / batchSize;
        if (length % batchSize > 0) {
            count++;
        }
        int startIndex = 0;
        int endIndex = batchSize;
        if (endIndex > length) {
            endIndex = length;
        }
        for (int i = 0; i < count; i++) {
            List<IMessage> batchItem = messages.subList(startIndex, endIndex);
            batchInsert(batchItem);
            startIndex = endIndex;
            endIndex += batchSize;
            if (endIndex > length) {
                endIndex = length;
            }
        }
        return true;
    }

    @Override
    public boolean flush(Set<String> splitIds) {
        int size = messageCache.flush(splitIds);
        return size > 0;
    }

    @Override
    public boolean flush(String... splitIds) {
        if (splitIds == null) {
            return true;
        }
        Set<String> splitIdSet = new HashSet<>();
        for (String splitId : splitIds) {
            splitIdSet.add(splitId);
        }
        return flush(splitIdSet);
    }

    protected abstract boolean batchInsert(List<IMessage> messages);

    @Override
    public void closeAutoFlush() {
        messageCache.closeAutoFlush();
    }

    @Override
    public boolean flushMessage(List<IMessage> messages) {

        boolean success = batchSave(messages);
        for (IMessage message : messages) {
            String queueId = message.getHeader().getQueueId();
            MessageOffset messageOffset = message.getHeader().getMessageOffset();
            ISource source = message.getHeader().getSource();
            String pipelineName = message.getHeader().getPiplineName();
            String sourceName = CheckPointManager.createSourceName(source, pipelineName);
            SourceState sourceState = this.sourceName2State.get(sourceName);
            if (sourceState == null) {
                sourceState = new SourceState();
                sourceState.setSourceName(sourceName);
            }
            sourceState.getQueueId2Offsets().put(queueId, messageOffset);
        }
        return success;
    }

    @Override
    public boolean checkpoint(Set<String> splitIds) {
        return flush(splitIds);
    }

    @Override
    public boolean checkpoint(String... splitIds) {
        if (splitIds == null) {
            return false;
        }
        Set<String> splitSet = new HashSet<>();
        for (String splitId : splitIds) {
            splitSet.add(splitId);
        }
        return checkpoint(splitSet);
    }

    @Override
    public boolean flush() {
        String name = getConfigureName();
        if (StringUtil.isEmpty(name)) {
            name = getClass().getName();
        }
        int size = messageCache.flush();
        if (size > 0) {
            logger.debug(String.format("%s finished flush data %d", name, size));
        }
        return true;
    }

    /**
     * 把message对象转化成jsonobject
     *
     * @param messageList
     * @return
     */
    protected List<JSONObject> convertJsonObjectFromMessage(List<IMessage> messageList) {
        List<JSONObject> messages = new ArrayList<>();
        for (IMessage message : messageList) {
            messages.add(message.getMessageBody());
        }
        return messages;
    }

    @Override
    public AbstractSink createStageChain(PipelineBuilder pipelineBuilder) {
        return this;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public IMessageCache<IMessage> getMessageCache() {
        return messageCache;
    }

    @Override
    public Map<String, MessageOffset> getFinishedQueueIdAndOffsets(CheckPointMessage checkPointMessage) {
        String pipelineName = null;
        if (checkPointMessage.getStreamOperator() instanceof IConfigurableIdentification) {
            IConfigurableIdentification configurable = (IConfigurableIdentification) checkPointMessage.getStreamOperator();
            pipelineName = configurable.getConfigureName();
        }
        SourceState sourceState = this.sourceName2State.get(
            CheckPointManager.createSourceName(checkPointMessage.getSource(), pipelineName));
        if (sourceState != null) {
            return sourceState.getQueueId2Offsets();
        }
        return new HashMap<>();
    }

    public void setMessageCache(
        IMessageCache<IMessage> messageCache) {
        this.messageCache = messageCache;
    }

    @Override
    public void atomicSink(ISystemMessage message){

    }

    @Override
    public void finish() throws Exception {
        this.closeAutoFlush();
    }

    @Override
    public boolean isFinished() throws Exception {
        return false;
    }
}
