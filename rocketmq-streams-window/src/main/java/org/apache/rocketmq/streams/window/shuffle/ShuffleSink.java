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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.WaterMarkNotifyMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.window.WindowConstants;
import org.apache.rocketmq.streams.window.minibatch.MiniBatchMsgCache;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.util.ShuffleUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShuffleSink extends
    AbstractSink {

    public static final String ORIGIN_QUEUE_ID = "origin_queue_id";
    public static final String ORIGIN_MESSAGE_TRACE_ID = "origin_request_id";
    public static final String SHUFFLE_OFFSET = "SHUFFLE_OFFSET";
    protected static final String SHUFFLE_QUEUE_ID = "SHUFFLE_QUEUE_ID";
    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";
    private static final Logger LOGGER = LoggerFactory.getLogger(ShuffleSink.class);
    private static final String SHUFFLE_TRACE_ID = "SHUFFLE_TRACE_ID";
    protected transient boolean isWindowTest = false;
    protected transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 分片转发channel
     */
    @ConfigurableReference protected AbstractSupportShuffleSink sink;
    @ConfigurableReference protected AbstractShuffleWindow window;
    /**
     * 所有的分片
     */
    protected transient List<ISplit<?, ?>> queueList;
    protected transient Map<String, ISplit<?, ?>> queueMap = new ConcurrentHashMap<>();
    protected transient MiniBatchMsgCache shuffleMsgCache;
    /**
     * 消息所属的window
     */
    protected String MSG_OWNER = "MSG_OWNER";

    @Override
    protected boolean initConfigurable() {
        super.initConfigurable();
        isWindowTest = ComponentCreator.getPropertyBooleanValue("window.fire.isTest");
        queueList = sink.getSplitList();
        Map<String, ISplit<?, ?>> tmp = new ConcurrentHashMap<>();
        for (ISplit<?, ?> queue : queueList) {
            tmp.put(queue.getQueueId(), queue);
        }
        this.queueMap = tmp;
        initMiniBatch();
        return true;

    }

    public void initMiniBatch() {
        shuffleMsgCache = new MiniBatchMsgCache(new ShuffleSink.MultiMsgMergerAndCompressFlushCallBack(), window, window);
        shuffleMsgCache.openAutoFlush();
    }

    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
        for (IMessage msg : messageList) {
            String shuffleKey = window.generateShuffleKey(msg);
            IMessage message = ShuffleUtil.createShuffleMsg(msg, shuffleKey);
            if (message == null) {
                continue;
            }
            addPropertyToMessage(msg, message.getMessageBody());
            Integer index = hash(shuffleKey);
            ISplit<?, ?> split = getSplit(index);
            shuffleMsgCache.addCache(new MutablePair<>(split, message));
        }
        if (isWindowTest) {
            long count = COUNT.addAndGet(messageList.size());
            System.out.println(window.getName() + " send shuffle msg count is " + count);
            shuffleMsgCache.flush();
        }
        return true;
    }

    public void finishBatchMsg(BatchFinishMessage batchFinishMessage) {
        if (sink != null) {
            this.flush();
            shuffleMsgCache.flush();
            for (ISplit<?, ?> split : queueList) {
                IMessage message = batchFinishMessage.getMsg().deepCopy();
                message.getMessageBody().put(ORIGIN_QUEUE_ID, message.getHeader().getQueueId());
                sink.batchAdd(message, split);
            }
            sink.flush();
        }
    }

    /**
     * @param msg
     */
    public void sendNotifyToShuffleSource(String sourceSplitId, JSONObject msg) {
        for (ISplit<?, ?> split : this.queueList) {
            WaterMarkNotifyMessage waterMarkNotifyMessage = new WaterMarkNotifyMessage();
            waterMarkNotifyMessage.setSourceSplitId(sourceSplitId);
            Message message = new Message(msg);
            message.getHeader().setSystemMessage(true);
            message.getMessageBody().put(ORIGIN_QUEUE_ID, sourceSplitId);
            message.setSystemMessage(waterMarkNotifyMessage);
            this.shuffleMsgCache.addCache(new MutablePair(split, message));
        }
        this.shuffleMsgCache.flush();

    }

    /**
     * 根据message生成shuffle key
     */
    //   protected abstract String generateShuffleKey(IMessage message);
    @Override
    public boolean checkpoint(Set<String> queueIds) {
        this.flush(queueIds);
        if (this.shuffleMsgCache != null) {
            this.shuffleMsgCache.flush(queueIds);
        }
        return true;
    }

    public ISplit<?, ?> getChannelQueue(String key) {
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

    /**
     * 如果需要额外的字段附加到shuffle前的message，通过实现这个子类增加
     */
    protected void addPropertyToMessage(IMessage oriMessage, JSONObject oriJson) {

    }

    public JSONObject createMsg(JSONArray messages, ISplit<?, ?> split) {
        JSONObject msg = new JSONObject();
        //分片id
        msg.put(SHUFFLE_QUEUE_ID, split.getQueueId());
        //合并的消息
        msg.put(SHUFFLE_MESSAGES, messages);
        //消息owner
        msg.put(MSG_OWNER, getDynamicPropertyValue());
        //
        try {
            List<String> traceList = new ArrayList<>();
            List<String> groupByList = new ArrayList<>();
            for (int i = 0; i < messages.size(); i++) {
                JSONObject object = messages.getJSONObject(i);
                groupByList.add(object.getString("SHUFFLE_KEY"));
                traceList.add(object.getString(ORIGIN_MESSAGE_TRACE_ID));
            }
            String traceInfo = StringUtils.join(traceList);
            String groupInfo = StringUtils.join(groupByList);
            msg.put(SHUFFLE_TRACE_ID, StringUtils.join(traceList));
            TraceUtil.debug(traceInfo, "origin message out", split.getQueueId(), groupInfo, getName());
        } catch (Exception e) {
            LOGGER.error("[{}] create message error", getName(), e);
        }
        return msg;
    }

    protected String getDynamicPropertyValue() {
        String dynamicPropertyValue = MapKeyUtil.createKey(window.getNameSpace(), window.getName(), window.getUpdateFlag() + "");
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";", "_");
        return dynamicPropertyValue;
    }

    public MiniBatchMsgCache getShuffleMsgCache() {
        return this.shuffleMsgCache;
    }

    public ISplit<?, ?> getSplit(Integer index) {
        return queueList.get(index);
    }

    public AbstractShuffleWindow getWindow() {
        return window;
    }

    public void setWindow(AbstractShuffleWindow window) {
        this.window = window;
    }

    public void setSink(AbstractSupportShuffleSink sink) {
        this.sink = sink;
    }

    public List<ISplit<?, ?>> getQueueList() {
        return queueList;
    }

    protected class MultiMsgMergerAndCompressFlushCallBack implements IMessageFlushCallBack<Pair<ISplit<?, ?>, IMessage>> {

        @Override
        public boolean flushMessage(List<Pair<ISplit<?, ?>, IMessage>> messages) {
            if (messages == null || messages.isEmpty()) {
                return true;
            }
            ISplit<?, ?> split = messages.get(0).getLeft();
            JSONArray allMsgs = new JSONArray();
            for (Pair<ISplit<?, ?>, IMessage> pair : messages) {
                IMessage message = pair.getRight();
                if (message.getHeader().isSystemMessage()) {
                    message.getMessageBody().put(WindowConstants.IS_SYSTEME_MSG, true);
                    message.getMessageBody().put(WindowConstants.SYSTEME_MSG, message.getSystemMessage());
                    message.getMessageBody().put(WindowConstants.SYSTEME_MSG_CLASS, message.getSystemMessage().getClass().getName());
                }
                allMsgs.add(message.getMessageBody());
            }
            JSONObject jsonObject = createMsg(allMsgs, split);
            sink.batchAdd(new Message(jsonObject), split);
            sink.flush(split.getQueueId());
            return true;
        }
    }
}
