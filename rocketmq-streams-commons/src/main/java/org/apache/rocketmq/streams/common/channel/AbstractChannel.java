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
package org.apache.rocketmq.streams.common.channel;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.SplitProgress;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;

/**
 * channel的抽象，实现了消息的封装，发送等核心逻辑
 */
public abstract class AbstractChannel extends BasedConfigurable implements IChannel<IChannel> {
    protected transient ISink sink;
    protected transient ISource source;
    protected transient AtomicBoolean hasCreated = new AtomicBoolean(false);

    protected abstract ISink createSink();

    protected abstract ISource createSource();

    @Override
    protected boolean initConfigurable() {
        create();
        return true;
    }

    protected void create() {
        if (hasCreated.compareAndSet(false, true)) {
            sink = createSink();
            source = createSource();
            sink.init();
            source.init();
        }

    }

    @Override
    public void getJsonObject(JSONObject jsonObject) {

        super.getJsonObject(jsonObject);
        String sinkValue = jsonObject.getString("sink");
        String sourceValue = jsonObject.getString("source");
        create();
        if (sourceValue != null) {
            source = InstantiationUtil.deserializeObject(Base64Utils.decode(sourceValue));
            if (source != null) {
                source.init();
            }

        }
        if (sinkValue != null) {
            sink = InstantiationUtil.deserializeObject(Base64Utils.decode(sinkValue));
            if (sink != null) {
                sink.init();
            }
        }

    }

    @Override public boolean checkpoint(Set<String> splitIds) {
        return sink.checkpoint(splitIds);
    }

    @Override public boolean checkpoint(String... splitIds) {
        return sink.checkpoint(splitIds);
    }

    @Override
    public boolean flush(String... splitIds) {
        return sink.flush(splitIds);
    }

    @Override
    protected void setJsonObject(JSONObject jsonObject) {
        super.setJsonObject(jsonObject);
        jsonObject.put("sink", Base64Utils.encode(InstantiationUtil.serializeObject(sink)));
        jsonObject.put("source", Base64Utils.encode(InstantiationUtil.serializeObject(source)));
    }

    public void removeSplit(Set<String> splitIds) {
        if (source instanceof AbstractSource) {
            ((AbstractSource) source).removeSplit(splitIds);
        }
    }

    public void addNewSplit(Set<String> splitIds) {
        if (source instanceof AbstractSource) {
            ((AbstractSource) source).addNewSplit(splitIds);
        }
    }

    @Override public Map<String, SplitProgress> getSplitProgress() {
        return source.getSplitProgress();
    }

    @Override
    public Map<String, MessageOffset> getFinishedQueueIdAndOffsets(CheckPointMessage checkPointMessage) {
        return sink.getFinishedQueueIdAndOffsets(checkPointMessage);
    }

    @Override public List<ISplit<?, ?>> fetchAllSplits() {
        return source.fetchAllSplits();
    }

    @Override
    public boolean flush(Set<String> splitIds) {
        return sink.flush(splitIds);
    }

    @Override
    public boolean flushMessage(List<IMessage> messages) {
        return sink.flushMessage(messages);
    }

    @Override
    public IChannel createStageChain(PipelineBuilder pipelineBuilder) {
        return this;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override
    public ISink getSink() {
        return sink;
    }

    @Override
    public ISource getSource() {
        return source;
    }

    @Override
    public boolean batchAdd(IMessage fieldName2Value, ISplit split) {
        return sink.batchAdd(fieldName2Value, split);
    }

    @Override
    public boolean batchAdd(IMessage fieldName2Value) {
        return sink.batchAdd(fieldName2Value);
    }

    @Override
    public boolean batchSave(List<IMessage> messages) {
        return sink.batchSave(messages);
    }

    @Override
    public boolean flush() {
        return sink.flush();
    }

    @Override
    public void openAutoFlush() {
        sink.openAutoFlush();
    }

    @Override
    public void closeAutoFlush() {
        sink.closeAutoFlush();
    }

    @Override
    public int getBatchSize() {
        return sink.getBatchSize();
    }

    @Override
    public void setBatchSize(int batchSize) {
        sink.setBatchSize(batchSize);
    }

    @Override
    public boolean start(IStreamOperator receiver) {
        return source.start(receiver);
    }

    @Override
    public String getGroupName() {
        return source.getGroupName();
    }

    @Override
    public void setGroupName(String groupName) {
        source.setGroupName(groupName);
    }

    @Override
    public int getMaxThread() {
        return source.getMaxThread();
    }

    @Override
    public void setMaxThread(int maxThread) {
        source.setMaxThread(maxThread);
    }

    @Override
    public void setMaxFetchLogGroupSize(int size) {
        source.setMaxFetchLogGroupSize(size);
    }

    @Override
    public long getCheckpointTime() {
        return source.getCheckpointTime();
    }

    public void setJsonData(Boolean isJsonData) {
        create();
        ((AbstractSource) source).setJsonData(isJsonData);
    }

    @Override
    public String getTopic() {
        return source.getTopic();
    }

    @Override
    public void setTopic(String topic) {
        source.setTopic(topic);
    }

    @Override
    public void atomicSink(ISystemMessage message) {

    }

}
