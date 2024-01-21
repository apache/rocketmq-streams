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
package org.apache.rocketmq.streams.common.topology.stages;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputChainStage<T extends IMessage> extends AbstractChainStage<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(OutputChainStage.class);
    /**
     * 可以关闭输出，在测试场景有效果.可以通过配置文件配置
     */
    @ENVDependence
    protected String closeOutput;

    @ConfigurableReference protected ISink<?> sink;

    @ConfigurableReference protected MetaData metaData;

    protected transient List<JSONObject> callbackMsgs;
    protected transient boolean isCallbackModel;//启动callback模式，输出直接给callback，不再写sink

    protected transient AtomicInteger count;

    @Override protected IMessage handleMessage(IMessage message, AbstractContext context) {
        if (StringUtil.isNotEmpty(closeOutput)) {
            String tmp = closeOutput.toLowerCase();
            if ("true".equals(tmp) || "false".equals(tmp)) {
                boolean value = Boolean.parseBoolean(tmp);
                if (value) {
                    return message;
                }
            } else {
                tmp = closeOutput;
                if (StringUtil.isNotEmpty(tmp)) {
                    if ("true".equals(tmp.toLowerCase())) {
                        return message;
                    }
                }
            }
        }
        boolean isWindowTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        if (isWindowTest) {
            LOGGER.info("[{}] output count is {}", this.getName(), count.incrementAndGet());
        }
        /*
         * 主要是输出可能影响线上数据，可以通过配置文件的开关，把所有的输出，都指定到一个其他输出中
         */
        if (isCallbackModel) {
            callbackMsgs.add(message.getMessageBody());
        } else {
            sink.batchAdd(message);
        }
        return message;
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        ISink<?> realSink = sink;

        if (message.getHeader().isNeedFlush()) {
            Set<String> queueIds = new HashSet<>();
            if (message.getHeader().getCheckpointQueueIds() != null) {
                queueIds.addAll(message.getHeader().getCheckpointQueueIds());
            }
            if (StringUtil.isNotEmpty(message.getHeader().getQueueId())) {
                queueIds.add(message.getHeader().getQueueId());
            }
            //todo
            realSink.checkpoint(queueIds);
        }
        CheckPointState checkPointState = new CheckPointState();
        checkPointState.setQueueIdAndOffset(realSink.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);

    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {

    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {

    }

    @Override public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage batchFinishMessage) {
        ISink<?> realSink = sink;
        if (batchFinishMessage.getMessageFinishCallBack(message) != null && isCallbackModel) {
            batchFinishMessage.getMessageFinishCallBack(message).finish(callbackMsgs);
        } else {
            realSink.flush();
        }

    }

    @Override public void startJob() {
        this.callbackMsgs = new ArrayList<>();
        this.count = new AtomicInteger(0);
    }

    @Override public void stopJob() {
        if (this.sink != null) {
            this.sink.destroy();
        }

    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public ISink<?> getSink() {
        return sink;
    }

    public void setSink(ISink<?> channel) {
        this.sink = channel;
        this.setNameSpace(channel.getNameSpace());
        this.setLabel(channel.getName());
    }

    public String getCloseOutput() {
        return closeOutput;
    }

    public void setCloseOutput(String closeOutput) {
        this.closeOutput = closeOutput;
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    public boolean isCallbackModel() {
        return isCallbackModel;
    }

    public void setCallbackModel(boolean callbackModel) {
        isCallbackModel = callbackModel;
    }
}
