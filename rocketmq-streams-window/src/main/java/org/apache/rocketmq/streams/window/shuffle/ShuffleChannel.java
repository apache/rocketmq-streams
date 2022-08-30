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
package org.apache.rocketmq.streams.window.shuffle;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.CompressUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

/**
 * 负责处理分片
 */
public class ShuffleChannel extends AbstractSystemChannel {

    protected static final Log LOG = LogFactory.getLog(ShuffleChannel.class);

    protected static final String SHUFFLE_QUEUE_ID = "SHUFFLE_QUEUE_ID";

    public static final String SHUFFLE_OFFSET = "SHUFFLE_OFFSET";

    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";

    /**
     * 消息所属的window
     */
    protected String MSG_OWNER = "MSG_OWNER";

    /**
     * 进入shuffle的时间，用来观察shuffle的时间延时
     */
    private static final String SHUFFLE_TIME = "SHUFFLE_TIME";

    protected ShuffleCache shuffleCache;

    protected Map<String, ISplit<?, ?>> queueMap = new ConcurrentHashMap<>();

    /**
     * 所有的分片
     */
    protected List<ISplit<?, ?>> queueList;

    protected AbstractShuffleWindow window;

    protected transient boolean isWindowTest = false;

    private transient HashMap<String, Future<?>> loadResult = new HashMap<>();

    protected transient AtomicLong COUNT = new AtomicLong(0);

    public ShuffleChannel(AbstractShuffleWindow window) {
        this.window = window;
        channelConfig = new HashMap<>();
        channelConfig.put(CHANNEL_PROPERTY_KEY_PREFIX, ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX);
        channelConfig.put(CHANNEL_TYPE, ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_TYPE);

        this.shuffleCache = new ShuffleCache(window);
        this.shuffleCache.init();
        this.shuffleCache.openAutoFlush();

    }

    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);

    @Override
    public void startChannel() {
        if (hasStart.compareAndSet(false, true)) {
            super.startChannel();
        }
    }

    /**
     * init shuffle channel
     */
    public void init() {
        this.consumer = createSource(window.getNameSpace(), window.getConfigureName());
        this.producer = createSink(window.getNameSpace(), window.getConfigureName());
        if (this.consumer == null || this.producer == null) {
            autoCreateShuffleChannel(window.getFireReceiver().getPipeline());
        }
        if (this.consumer == null) {
            return;
        }
        if (this.consumer instanceof AbstractSource) {
            ((AbstractSource) this.consumer).setJsonData(true);
        }
        if (producer != null && (queueList == null || queueList.size() == 0)) {
            queueList = producer.getSplitList();
            Map<String, ISplit<?, ?>> tmp = new ConcurrentHashMap<>();
            for (ISplit<?, ?> queue : queueList) {
                tmp.put(queue.getQueueId(), queue);
            }
            this.queueMap = tmp;
        }
        isWindowTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        //
        if (window.getEventTimeManager() != null) {
            window.getEventTimeManager().setSource(this.consumer);
        }
    }

    /**
     * 接收到分片信息，如果是系统消息，做缓存刷新，否则把消息放入缓存，同时计算存储的有效性
     *
     * @param oriMessage
     * @param context
     * @return
     */
    @Override
    public Object doMessage(IMessage oriMessage, AbstractContext context) {
        if (oriMessage.getHeader().isSystemMessage()) {
            doSystemMessage(oriMessage, context);
            return null;

        }
        if (oriMessage.getMessageBody().getBooleanValue(WindowCache.IS_COMPRESSION_MSG)) {
            byte[] bytes = oriMessage.getMessageBody().getBytes(WindowCache.COMPRESSION_MSG_DATA);
            String msgStr = CompressUtil.unGzip(bytes);
            oriMessage.setMessageBody(JSONObject.parseObject(msgStr));
        }
        /**
         * 过滤不是这个window的消息，一个shuffle通道，可能多个window共享，这里过滤掉非本window的消息
         */
        boolean isFilter = filterNotOwnerMessage(oriMessage);
        if (isFilter) {
            return null;
        }
        String queueId = oriMessage.getHeader().getQueueId();
        JSONArray messages = oriMessage.getMessageBody().getJSONArray(SHUFFLE_MESSAGES);
        if (messages == null) {
            return null;
        }

        for (Object obj : messages) {
            IMessage message = new Message((JSONObject) obj);
            message.getHeader().setQueueId(queueId);
            message.getMessageBody().put(SHUFFLE_OFFSET, oriMessage.getHeader().getOffset());
            window.updateMaxEventTime(message);
            if (isRepeatedMessage(message, queueId)) {
                continue;
            }
            List<WindowInstance> windowInstances = window.queryOrCreateWindowInstance(message, queueId);
            if (windowInstances == null || windowInstances.size() == 0) {
                LOG.warn("the message is out of window instance, the message is discard");
                continue;
            }
            for (WindowInstance windowInstance : windowInstances) {
                //new instance, not need load data from remote
                if (windowInstance.isNewWindowInstance()) {
                    windowInstance.setNewWindowInstance(false);
                }
            }

            message.getMessageBody().put(WindowInstance.class.getSimpleName(), windowInstances);
            message.getMessageBody().put(AbstractWindow.class.getSimpleName(), window);

            if (DebugWriter.getDebugWriter(window.getConfigureName()).isOpenDebug()) {
                List<IMessage> msgs = new ArrayList<>();
                msgs.add(message);
                DebugWriter.getDebugWriter(window.getConfigureName()).writeShuffleReceiveBeforeCache(window, msgs, queueId);
            }

            for (WindowInstance windowInstance : windowInstances) {
                window.getWindowFireSource().updateWindowInstanceLastUpdateTime(windowInstance);
            }
            shuffleCache.batchAdd(message);
        }
        if (isWindowTest) {
            long count = COUNT.addAndGet(messages.size());
            System.out.println(window.getConfigureName() + " receive shuffle msg count is " + count);
        }
        return null;
    }

    //加载状态数据到本地存储
    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        Set<String> splitIds = newSplitMessage.getSplitIds();

        Future<?> future = this.window.getStorage().load(splitIds);
        for (String splitId : splitIds) {
             this.loadResult.put(splitId, future);
        }

        if (message.getHeader().isSystemMessage() && window.getFireReceiver() == null) {
            return;
        }

        window.getFireReceiver().doMessage(message, context);
    }


    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        //去掉缓存中的
        Set<String> queueIds = removeSplitMessage.getSplitIds();
        if (queueIds != null) {
            for (String queueId : queueIds) {
                window.clearCache(queueId);
            }
        }
        window.getFireReceiver().doMessage(message, context);
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        if (message.getHeader().isNeedFlush()) {
            this.flush(message.getHeader().getCheckpointQueueIds());
            window.getStorage().flush(new ArrayList<>(message.getHeader().getCheckpointQueueIds()));
        }
        CheckPointState checkPointState = new CheckPointState();
        checkPointState.setQueueIdAndOffset(this.shuffleCache.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);
    }


    protected void doSystemMessage(IMessage oriMessage, AbstractContext context) {
        ISystemMessage systemMessage = oriMessage.getSystemMessage();
        if (systemMessage instanceof CheckPointMessage) {
            this.checkpoint(oriMessage, context, (CheckPointMessage) systemMessage);
        } else if (systemMessage instanceof NewSplitMessage) {
            this.addNewSplit(oriMessage, context, (NewSplitMessage) systemMessage);
        } else if (systemMessage instanceof RemoveSplitMessage) {
            this.removeSplit(oriMessage, context, (RemoveSplitMessage) systemMessage);
        } else if (systemMessage instanceof BatchFinishMessage) {
            System.out.println("start fire window by fininsh flag " + oriMessage.getHeader().getQueueId());
            this.batchMessageFinish(oriMessage, context, (BatchFinishMessage) systemMessage);
        } else {
            throw new RuntimeException("can not support this system message " + systemMessage.getClass().getName());
        }
        afterFlushCallback(oriMessage, context);
    }

    /**
     * if the message offset is old filter the repeate message
     *
     * @param message
     * @param queueId
     * @return
     */
    protected boolean isRepeatedMessage(IMessage message, String queueId) {
        boolean isOrigOffsetLong = message.getMessageBody().getBoolean(WindowCache.ORIGIN_QUEUE_IS_LONG);
        String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
        String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);

        //由storage统一缓存，方便管理一致性
        String maxOffset = this.window.getStorage().getMaxOffset(queueId, window.getConfigureName(), oriQueueId);

        if (maxOffset != null) {
            if (!MessageOffset.greateThan(oriOffset, maxOffset, isOrigOffsetLong)) {
                System.out.println("the message offset is old, the message is discard ");
                return true;
            }
        }
        return false;
    }

    @Override
    protected Map<String, String> getChannelConfig() {
        return channelConfig;
    }

    @Override
    protected void putDynamicPropertyValue(Set<String> dynamicPropertySet, Properties properties) {
        String groupName = "groupName";
        if (!dynamicPropertySet.contains(groupName)) {
            properties.put(groupName, getDynamicPropertyValue());
        }
        if (!dynamicPropertySet.contains("tags")) {
            properties.put("tags", getDynamicPropertyValue());
        }
    }

    /**
     * 1个pipeline一个 shuffle topic
     *
     * @param topic
     * @param pipeline
     * @return
     */
    @Override
    protected String createShuffleTopic(String topic, ChainPipeline pipeline) {
        return "shuffle_" + topic + "_" + pipeline.getSource().getNameSpace().replaceAll("\\.", "_") + "_" + pipeline
            .getConfigureName().replaceAll("\\.", "_").replaceAll(";", "_");
    }

    /**
     * 一个window 一个channel
     *
     * @param pipeline
     * @return
     */
    @Override
    protected String createShuffleChannelName(ChainPipeline pipeline) {
        return getDynamicPropertyValue();
    }

    /**
     * 和pipeline namespace 相同
     *
     * @param pipeline
     * @return
     */
    @Override
    protected String createShuffleChannelNameSpace(ChainPipeline pipeline) {
        return pipeline.getSource().getNameSpace();
    }

    @Override
    public String getConfigureName() {
        return window.getConfigureName() + "_shuffle";
    }

    @Override
    public String getNameSpace() {
        return window.getNameSpace();
    }

    @Override
    public String getType() {
        return Pipeline.TYPE;
    }

    public ISplit getSplit(Integer index) {
        return queueList.get(index);
    }

    public JSONObject createMsg(JSONArray messages, ISplit split) {
        JSONObject msg = new JSONObject();
        //分片id
        msg.put(SHUFFLE_QUEUE_ID, split.getQueueId());
        //合并的消息
        msg.put(SHUFFLE_MESSAGES, messages);
        //消息owner
        msg.put(MSG_OWNER, getDynamicPropertyValue());
        //
        msg.put(SHUFFLE_TIME, System.currentTimeMillis());

        return msg;
    }

    public JSONArray getMsgs(JSONObject msg) {
        return msg.getJSONArray(SHUFFLE_MESSAGES);
    }

    public ISplit getChannelQueue(String key) {
        int index = hash(key);
        return queueList.get(index);
    }

    public int hash(Object key) {
        int mValue = queueList.size();
        int h = 0;
        if (key != null) {
            h = key.hashCode() ^ (h >>> 16);
            if (h < 0) {
                h = -h;
            }
        }
        return h % mValue;
    }

    public void flush(Set<String> checkpointQueueIds) {
        shuffleCache.flush(checkpointQueueIds);
    }

    /**
     * 每次checkpoint的回调函数，默认是空实现，子类可以扩展实现
     *
     * @param oriMessage
     * @param context
     */
    protected void afterFlushCallback(IMessage oriMessage, AbstractContext context) {
    }

    /**
     * shuffle 获取数据，插入缓存前的回调函数，默认空实现，可以子类覆盖扩展
     *
     * @param oriMessage
     * @param message
     */
    protected void beforeBatchAdd(IMessage oriMessage, IMessage message) {
    }

    /**
     * 过滤掉不是这个window的消息
     *
     * @param oriMessage
     * @return
     */
    protected boolean filterNotOwnerMessage(IMessage oriMessage) {
        String owner = oriMessage.getMessageBody().getString(MSG_OWNER);
        if (owner != null && owner.equals(getDynamicPropertyValue())) {
            return false;
        }
        return true;
    }

    @Override
    protected String getDynamicPropertyValue() {
        String dynamicPropertyValue = MapKeyUtil.createKey(window.getNameSpace(), window.getConfigureName(), window.getUpdateFlag() + "");
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";", "_");
        return dynamicPropertyValue;
    }

    @Override
    protected int getShuffleSplitCount(AbstractSupportShuffleSink shuffleSink) {
        int splitNum = shuffleSink.getSplitNum();
        return splitNum > 0 ? splitNum : 32;
    }


    public List<ISplit<?, ?>> getQueueList() {
        return queueList;
    }

    public AbstractShuffleWindow getWindow() {
        return window;
    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage batchFinishMessage) {
        if (window.supportBatchMsgFinish()) {
            shuffleCache.flush(message.getHeader().getQueueId());
            List<String> queueIds = new ArrayList<>();
            queueIds.add(message.getHeader().getQueueId());


            window.getStorage().flush(queueIds);

            window.getWindowFireSource().fireWindowInstance(message.getHeader().getQueueId());
            IMessage cpMsg = batchFinishMessage.getMsg().copy();
            window.getFireReceiver().doMessage(cpMsg, context);
        }

    }

    public HashMap<String, Future<?>> getLoadResult() {
        return loadResult;
    }
}