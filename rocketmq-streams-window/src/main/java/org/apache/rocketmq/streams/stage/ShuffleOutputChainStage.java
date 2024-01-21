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
package org.apache.rocketmq.streams.stage;

import com.alibaba.fastjson.JSONObject;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.model.SQLCompileContextForSource;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.shuffle.ShuffleManager;
import org.apache.rocketmq.streams.window.shuffle.ShuffleSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleOutputChainStage<T extends IMessage> extends AbstractChainStage<T> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleOutputChainStage.class);
    protected transient ShuffleSink shuffleSink;
    @ConfigurableReference protected AbstractShuffleWindow window;
    protected transient AtomicBoolean isFirstMessage = new AtomicBoolean(true);

    public ShuffleOutputChainStage() {
        this.setDiscription("Shuffle_Send");
        super.entityName = "window";
    }

    @Override
    protected IMessage handleMessage(IMessage message, AbstractContext context) {
        shuffleSend(message, context);
        if (!window.isSynchronous()) {
            context.breakExecute();
        }

        return message;
    }

    @Override public void startJob() {
        ISource<?> source = ((ChainPipeline<?>) getPipeline()).getSource();
        if (source == null) {
            /**
             * sql场景，会用嵌套的pipelinebuilder 创建，source可能无法传递过来
             */
            source = SQLCompileContextForSource.getInstance().get();
        }
        Pair<ISource<?>, AbstractSupportShuffleSink> shuffleSourceAndSink = ShuffleManager.createOrGetShuffleSourceAndSink(source, window);

        if (shuffleSourceAndSink == null) {
            return;
        }
        ShuffleSink shuffleSink = new ShuffleSink();
        shuffleSink.setNameSpace(getNameSpace());
        shuffleSink.setSink(shuffleSourceAndSink.getRight());
        shuffleSink.setWindow(window);
        shuffleSink.init();
        window.start();
        window.setShuffleSink(shuffleSink);
        this.shuffleSink = shuffleSink;
    }

    @Override public void stopJob() {
        super.destroy();
        this.shuffleSink.destroy();
        this.window.destroy();
        this.isFirstMessage.set(true);
    }

    protected void shuffleSend(IMessage message, AbstractContext context) {
        if (isFirstMessage.compareAndSet(true, false) && !message.getHeader().isSystemMessage()) {
            //当收到第一条消息的时候
            JSONObject msg = new JSONObject();
            msg.put(window.getTimeFieldName(), message.getMessageBody().getString(window.getTimeFieldName()));
            shuffleSink.sendNotifyToShuffleSource(message.getHeader().getQueueId(), msg);
        }

        if (StringUtils.isNotEmpty(window.getSizeVariable())) {
            if (message.getMessageBody().containsKey(window.getSizeVariable())) {
                try {
                    window.setSizeInterval(window.getSizeAdjust() * message.getMessageBody().getInteger(window.getSizeVariable()));
                } catch (Exception e) {
                    LOGGER.error("failed in getting the size value, message = " + message.toString(), e);
                }
            }
        }
        if (StringUtils.isNotEmpty(window.getSlideVariable())) {
            if (message.getMessageBody().containsKey(window.getSlideVariable())) {
                try {
                    window.setSlideInterval(window.getSlideAdjust() * message.getMessageBody().getInteger(window.getSlideVariable()));
                } catch (Exception e) {
                    LOGGER.error("failed in getting the slide value, message = " + message.toString(), e);
                }
            }
        }
        JSONObject msg = message.getMessageBody();
        msg.put(MessageHeader.class.getSimpleName(), message.getHeader());
        msg.put(AbstractWindow.class.getSimpleName(), this);
        shuffleSink.batchAdd(message);
        TraceUtil.debug(message.getHeader().getTraceId(), "origin message in");
    }

    @Override
    public boolean isAsyncNode() {
        if (window != null) {
            return !window.isSynchronous();
        }
        return false;
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        if (shuffleSink == null) {//over window windowcache  is null
            return;
        }
        if (message.getHeader().isNeedFlush()) {
            if (shuffleSink != null && message.getHeader().getCheckpointQueueIds() != null && message.getHeader().getCheckpointQueueIds().size() > 0) {
                shuffleSink.checkpoint(message.getHeader().getCheckpointQueueIds());
            } else {
                if (shuffleSink != null) {
                    Set<String> queueIds = new HashSet<>();
                    queueIds.add(message.getHeader().getQueueId());
                    shuffleSink.checkpoint(queueIds);
                }

            }

        }
        CheckPointState checkPointState = new CheckPointState();
        checkPointState.setQueueIdAndOffset(shuffleSink.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {

    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {

    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {
        checkPointMessage.getMsg().getMessageBody().put(WindowConstants.ORIGIN_MESSAGE_HEADER, JSONObject.toJSONString(message.getHeader()));
        shuffleSink.finishBatchMsg(checkPointMessage);
    }

    public IWindow getWindow() {
        return window;
    }

    public void setWindow(AbstractShuffleWindow window) {
        if (window == null) {
            throw new RuntimeException("can not get shuffle window");
        }
        this.window = window;
    }

    @Override
    public String getEntityName() {
        return super.entityName;
    }

}
