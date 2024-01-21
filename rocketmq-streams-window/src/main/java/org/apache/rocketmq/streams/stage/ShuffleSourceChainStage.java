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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.WaterMarkNotifyMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.interfaces.INeedAalignWaiting;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.SQLCompileContextForSource;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.CompressUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator;
import org.apache.rocketmq.streams.window.shuffle.ShuffleCache;
import org.apache.rocketmq.streams.window.shuffle.ShuffleManager;
import org.apache.rocketmq.streams.window.sqlcache.impl.SQLElement;
import org.apache.rocketmq.streams.window.storage.ShufflePartitionManager;
import org.apache.rocketmq.streams.window.trigger.WindowTrigger;
import org.apache.rocketmq.streams.window.util.ShuffleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleSourceChainStage<T extends IMessage> extends AbstractChainStage<T> {
    public static final String SHUFFLE_OFFSET = "SHUFFLE_OFFSET";
    public static final String IS_COMPRESSION_MSG = "_is_compress_msg";
    public static final String COMPRESSION_MSG_DATA = "_compress_msg";
    public static final String MSG_FROM_SOURCE = "msg_from_source";
    public static final String ORIGIN_OFFSET = "origin_offset";
    public static final String ORIGIN_QUEUE_ID = "origin_queue_id";
    public static final String ORIGIN_QUEUE_IS_LONG = "origin_offset_is_LONG";
    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";
    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleSourceChainStage.class);
    private static final String SHUFFLE_TRACE_ID = "SHUFFLE_TRACE_ID";
    /**
     * 消息所属的window
     */
    protected String MSG_OWNER = "MSG_OWNER";
    protected transient ISource<?> shuffleSource;//消费shuffle的数据
    @ConfigurableReference protected AbstractShuffleWindow window;

    protected transient ShuffleCache shuffleCache;//消费的数据先攒批，然后调回调接口IShuffleCallback做窗口计算(accumulate)
    protected transient WindowTrigger windowTrigger;//触发窗口，当窗口触发时调回调接口IShuffleCallback做fire，fire后会调回调接口clearWindowInstance清理资源，sendFireMessage发送结果给后续的节点

    /**
     * 每个分片，已经确定处理的最大offset
     */
    protected transient Map<String, String> split2MaxOffsets;
    protected transient boolean isWindowTest = false;
    protected transient AtomicLong COUNT;
    /**
     * 当前管理的分片
     */
    private Set<String> currentQueueIds;

    public ShuffleSourceChainStage() {
        setDiscription("Shuffle_Receive");
    }

    @Override public void startJob() {
        COUNT = new AtomicLong(0);
        this.split2MaxOffsets = new HashMap<>();
        this.window.setFireReceiver(getReceiverAfterCurrentNode());
        this.shuffleCache = new ShuffleCache(window);
        this.shuffleCache.init();
        this.shuffleCache.openAutoFlush();

        this.windowTrigger = new WindowTrigger(window);
        this.windowTrigger.start();
        window.setWindowTrigger(windowTrigger);
        this.window.start();
        window.getEventTimeManager().setSource(((ChainPipeline) this.getPipeline()).getSource());
        initShuffleSource();
        if (shuffleSource != null) {//view场景，有可能source为null
            shuffleSource.start((IStreamOperator<IMessage, AbstractContext>) (message, context) -> {
                consumeShuffleMessage(message, context);
                stageMetric.outCalculate();
                return null;
            });
        }

    }

    @Override public void stopJob() {
        this.shuffleCache.destroy();
        this.windowTrigger.destroy();
        this.shuffleSource.destroy();
        this.window.destroy();
    }

    protected void initShuffleSource() {
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
        this.shuffleSource = shuffleSourceAndSink.getLeft();
        this.shuffleSource.init();
    }

    protected void consumeShuffleMessage(IMessage oriMessage, AbstractContext context) {
        if (oriMessage.getHeader().isSystemMessage()) {
            doSystemMessage(oriMessage, context);
            return;

        }
        if (oriMessage.getMessageBody().getBooleanValue(IS_COMPRESSION_MSG)) {
            byte[] bytes = oriMessage.getMessageBody().getBytes(COMPRESSION_MSG_DATA);
            String msgStr = CompressUtil.unGzip(bytes);
            oriMessage.setMessageBody(JSONObject.parseObject(msgStr));
        }
        /*
          过滤不是这个window的消息，一个shuffle通道，可能多个window共享，这里过滤掉非本window的消息
         */
        boolean isFilter = filterNotOwnerMessage(oriMessage);
        if (isFilter) {
            return;
        }
        String queueId = oriMessage.getHeader().getQueueId();
        JSONArray messages = oriMessage.getMessageBody().getJSONArray(SHUFFLE_MESSAGES);
        if (messages == null) {
            return;
        }

        String traceId = oriMessage.getMessageBody().getString(SHUFFLE_TRACE_ID);
        if (!StringUtil.isEmpty(traceId)) {
            TraceUtil.debug(traceId, "shuffle message in", "received message size:" + messages.size());
        }
        for (Object obj : messages) {

            IMessage message = restoreMsg((JSONObject) obj);

            if (message.getHeader().isSystemMessage()) {
                doSystemMessage(message, context);
                continue;
            }
            stageMetric.startCalculate(message);
            message.getHeader().setQueueId(queueId);
            message.getMessageBody().put(SHUFFLE_OFFSET, oriMessage.getHeader().getOffset());
            window.updateMaxEventTime(message);
            if (isRepeatMessage(message, queueId)) {
                continue;
            }
            List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(message, queueId);
            if (windowInstances == null || windowInstances.isEmpty()) {
                LOGGER.warn("[{}][{}] WindowInstance_CreateNull_Error_TimeOn({})]", IdUtil.instanceId(), NameCreator.getFirstPrefix(window.getName(), IWindow.TYPE), DateUtil.longToString(WindowInstance.getOccurTime(this.window, message)));
                continue;
            }
            for (WindowInstance windowInstance : windowInstances) {
                String windowInstanceId = windowInstance.createWindowInstanceId();
                //new instance, not need load data from remote
                if (windowInstance.isNewWindowInstance()) {
                    window.getSqlCache().addCache(new SQLElement(windowInstance.getSplitId(), windowInstanceId, ORMUtil.createBatchReplaceSQL(windowInstance)));
                    windowInstance.setNewWindowInstance(false);
                    ShufflePartitionManager.getInstance().setWindowInstanceFinished(windowInstance.createWindowInstanceId());
                }
            }

            message.getMessageBody().put(WindowInstance.class.getSimpleName(), windowInstances);
            message.getMessageBody().put(AbstractWindow.class.getSimpleName(), window);

//            if (DebugWriter.getDebugWriter(window.getName()).isOpenDebug()) {
//                List<IMessage> msgs = new ArrayList<>();
//                msgs.add(message);
//                DebugWriter.getDebugWriter(window.getName()).writeShuffleReceiveBeforeCache(window, msgs, queueId);
//            }

            for (WindowInstance windowInstance : windowInstances) {
                windowTrigger.updateWindowInstanceLastUpdateTime(windowInstance);
            }
            shuffleCache.batchAdd(message);
        }
        if (isWindowTest) {
            long count = COUNT.addAndGet(messages.size());
            LOGGER.debug("[{}] receive shuffle msg count is {}", window.getName(), count);
        }

    }

    protected IMessage restoreMsg(JSONObject obj) {
        Message message = new Message(obj);
        if (obj.get(WindowConstants.IS_SYSTEME_MSG) != null && obj.getBoolean(WindowConstants.IS_SYSTEME_MSG)) {
            message.getHeader().setSystemMessage(true);
            message.setSystemMessage((ISystemMessage) obj.getObject(WindowConstants.SYSTEME_MSG, ReflectUtil.forClass(obj.getString(WindowConstants.SYSTEME_MSG_CLASS))));
            message.getHeader().setQueueId(obj.getString(ORIGIN_QUEUE_ID));
        }
        return message;
    }

    /**
     * do system message
     *
     * @param oriMessage
     * @param context
     */
    protected void doSystemMessage(IMessage oriMessage, AbstractContext context) {
        ISystemMessage systemMessage = oriMessage.getSystemMessage();
        MessageHeader messageHeader = ShuffleUtil.getMessageHeader(oriMessage.getMessageBody());
        if (messageHeader != null) {
            oriMessage.setHeader(messageHeader);
        }
        if (systemMessage.getSystemMessageType() == ISystemMessage.CHECK_POINT) {
            this.checkpoint(oriMessage, context, (CheckPointMessage) systemMessage);
        } else if (systemMessage.getSystemMessageType() == ISystemMessage.SPLIT_ADD) {
            this.addNewSplit(oriMessage, context, (NewSplitMessage) systemMessage);
        } else if (systemMessage.getSystemMessageType() == ISystemMessage.SPLIT_REMOVE) {
            this.removeSplit(oriMessage, context, (RemoveSplitMessage) systemMessage);
        } else if (systemMessage.getSystemMessageType() == ISystemMessage.BATCH_FINISH) {
            boolean canSend = true;
            if (this.window instanceof INeedAalignWaiting) {
                INeedAalignWaiting needAalignWaiting = (INeedAalignWaiting) window;
                canSend = needAalignWaiting.alignWaiting(oriMessage);
            }
            if (!canSend) {
                context.breakExecute();
                return;
            }
            this.batchMessageFinish(oriMessage, context, (BatchFinishMessage) systemMessage);
        } else if (systemMessage.getSystemMessageType() == ISystemMessage.WATER_MARK) {
            this.notifyWaterMarkMessage(oriMessage, context, (WaterMarkNotifyMessage) systemMessage);
        } else {
            throw new RuntimeException("can not support this system message " + systemMessage.getClass().getName() + "," + systemMessage.getSystemMessageType());
        }
        afterFlushCallback(oriMessage, context);
    }

    protected void notifyWaterMarkMessage(IMessage message, AbstractContext context, WaterMarkNotifyMessage message1) {
        window.updateMaxEventTime(message);

    }

    /**
     * load ori split consume offset
     *
     * @param newSplitMessage
     */
    protected void loadSplitProgress(NewSplitMessage newSplitMessage) {
        for (String queueId : newSplitMessage.getSplitIds()) {
            Map<String, String> result = window.getWindowMaxValueManager().loadOffsets(window.getName(), queueId);
            if (result != null) {
                this.split2MaxOffsets.putAll(result);
            }
        }
    }

    @Override public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        this.currentQueueIds = newSplitMessage.getCurrentSplitIds();
        loadSplitProgress(newSplitMessage);

        List<WindowInstance> allWindowInstances = WindowInstance.queryAllWindowInstance(DateUtil.getCurrentTimeString(), window, newSplitMessage.getSplitIds());
        if (CollectionUtil.isNotEmpty(allWindowInstances)) {
            for (WindowInstance windowInstance : allWindowInstances) {
                windowInstance.setNewWindowInstance(false);
                window.registerWindowInstance(windowInstance);
                windowTrigger.registFireWindowInstanceIfNotExist(windowInstance, window);
                String queueId = windowInstance.getSplitId();
                window.getStorage().loadSplitData2Local(queueId, windowInstance.createWindowInstanceId(), window.getWindowBaseValueClass(), new WindowOperator.WindowRowOperator(windowInstance, queueId, window));
                window.initWindowInstanceMaxSplitNum(windowInstance);
            }

        } else {
            for (String queueId : newSplitMessage.getSplitIds()) {
                ShufflePartitionManager.getInstance().setSplitFinished(queueId);
            }
        }
        getReceiverAfterCurrentNode().doMessage(message, context);
    }

    @Override public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        this.currentQueueIds = removeSplitMessage.getCurrentSplitIds();
        Set<String> queueIds = removeSplitMessage.getSplitIds();
        if (queueIds != null) {
            for (String queueId : queueIds) {
                ShufflePartitionManager.getInstance().setSplitInValidate(queueId);
                window.clearCache(queueId);
            }
            window.getWindowMaxValueManager().removeKeyPrefixFromLocalCache(queueIds);
            //window.getWindowFireSource().removeSplit(queueIds);
        }
        getReceiverAfterCurrentNode().doMessage(message, context);
    }

    @Override public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        if (message.getHeader().isNeedFlush()) {
            shuffleCache.flush(message.getHeader().getCheckpointQueueIds());
            window.getSqlCache().flush(message.getHeader().getCheckpointQueueIds());
        }
        CheckPointState checkPointState = new CheckPointState();
        checkPointState.setQueueIdAndOffset(this.shuffleCache.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);
    }

    /**
     * if the message offset is old filter the repeate message
     *
     * @param message
     * @param queueId
     * @return
     */
    protected boolean isRepeatMessage(IMessage message, String queueId) {
        boolean isOrigOffsetLong = message.getMessageBody().getBoolean(ORIGIN_QUEUE_IS_LONG);
        String oriQueueId = message.getMessageBody().getString(ORIGIN_QUEUE_ID);
        String oriOffset = message.getMessageBody().getString(ORIGIN_OFFSET);
        String key = MapKeyUtil.createKey(window.getName(), queueId, oriQueueId);
        String offset = this.split2MaxOffsets.get(key);
        if (offset != null) {
            MessageOffset messageOffset = new MessageOffset(oriOffset, isOrigOffsetLong);
            if (!messageOffset.greaterThan(offset)) {
                LOGGER.debug("[{}] the message offset is old, the message is discard ", this.getName());
                return true;
            }
        }
        return false;
    }

    @Override public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage batchFinishMessage) {
        if (window.supportBatchMsgFinish()) {
            // System.out.println("start fire window by fininsh flag ");
            long startTime = System.currentTimeMillis();
            Set<String> splitIds = new HashSet<>();
            splitIds.add(message.getHeader().getQueueId());
            if (shuffleCache != null) {
                shuffleCache.flush(splitIds);
            }
            window.getSqlCache().flush(splitIds);
            windowTrigger.fireWindowInstance(message.getHeader().getQueueId());
            IMessage cpMsg = batchFinishMessage.getMsg().copy();
            getReceiverAfterCurrentNode().doMessage(cpMsg, context);
            LOGGER.debug("[{}] batch message finish cost is {}", this.getName(), (System.currentTimeMillis() - startTime));
        }

    }

    /**
     * 过滤掉不是这个window的消息
     *
     * @param oriMessage
     * @return
     */
    protected boolean filterNotOwnerMessage(IMessage oriMessage) {
        String owner = oriMessage.getMessageBody().getString(MSG_OWNER);
        return owner == null || !owner.equals(getDynamicPropertyValue());
    }

    /**
     * 每次checkpoint的回调函数，默认是空实现，子类可以扩展实现
     *
     * @param oriMessage
     * @param context
     */
    protected void afterFlushCallback(IMessage oriMessage, AbstractContext context) {
    }

    @Override protected T handleMessage(T t, AbstractContext context) {
        return null;
    }

    @Override public boolean isAsyncNode() {
        return false;
    }

    protected String getDynamicPropertyValue() {
        String dynamicPropertyValue = MapKeyUtil.createKey(window.getNameSpace(), window.getName(), window.getUpdateFlag() + "");
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";", "_");
        return dynamicPropertyValue;
    }

    public void setShuffleSource(ISource source) {
        this.shuffleSource = source;
    }

    public void setWindow(AbstractShuffleWindow window) {
        this.window = window;
    }

    public Set<String> getCurrentQueueIds() {
        return currentQueueIds;
    }

    public void setCurrentQueueIds(Set<String> currentQueueIds) {
        this.currentQueueIds = currentQueueIds;
    }
}
