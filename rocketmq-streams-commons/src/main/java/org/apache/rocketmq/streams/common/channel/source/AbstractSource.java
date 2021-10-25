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
package org.apache.rocketmq.streams.common.channel.source;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.interfaces.ILifeCycle;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * channel的抽象，实现了消息的封装，发送等核心逻辑
 */
public abstract class AbstractSource extends BasedConfigurable implements ISource<AbstractSource>, ILifeCycle {

    public static String CHARSET = "UTF-8";

    protected Boolean isJsonData = true;//输入的消息是否为json
    protected Boolean msgIsJsonArray = false;//输入的消息是否为json array
    @ENVDependence
    protected String groupName;//group name
    protected int maxThread = Runtime.getRuntime().availableProcessors();
    @ENVDependence
    protected String topic = "";
    /**
     * 多长时间做一次checkpoint
     */
    protected long checkpointTime = 1000 * 60 * 2;
    /**
     * 是否是批量消息，批量消息会一批做一次checkpoint，比如通过数据库加载的批消息
     */
    protected boolean isBatchMessage = false;
    /**
     * 每次拉取的最大条数，多用于消息队列
     */
    protected int maxFetchLogGroupSize = 100;
    protected List<String> logFingerprintFields;//log fingerprint to filter msg quickly


    /**
     * 数据源投递消息的算子，此算子用来接收source的数据，做处理
     */
    protected volatile transient IStreamOperator receiver;
    /**
     * 开启mock模式，则会收集mock数据，如果数据源没数据，则会发送mock数据
     */
    protected transient volatile Boolean openMock = false;

    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);

    /**
     * 做checkpoint的管理
     */
    protected transient CheckPointManager checkPointManager = new CheckPointManager();

    @Override
    protected boolean initConfigurable() {
        hasStart = new AtomicBoolean(false);
        openMock = false;
        return super.initConfigurable();
    }

    @Override
    public boolean start(IStreamOperator receiver) {
        this.receiver = receiver;
        boolean isStartSucess = true;
        if (hasStart.compareAndSet(false, true)) {
            isStartSucess = startSource();
        }
        return isStartSucess;
    }

    /**
     * 启动 source
     *
     * @return
     */
    protected abstract boolean startSource();

    public AbstractSource() {
        setType(ISource.TYPE);
    }

    /**
     * 提供单条消息的处理逻辑，默认不会加入checkpoint
     *
     * @param message
     * @return
     */
    public AbstractContext doReceiveMessage(JSONObject message, boolean needSetCheckPoint, String queueId,
                                            String offset) {
        Message msg = createMessage(message, queueId, offset, needSetCheckPoint);
        AbstractContext context = executeMessage(msg);
        return context;
    }

    /**
     * 处理消息，并且判断是否需要进行加入check point表识别
     *
     * @param message
     * @param needSetCheckPoint
     * @return
     */
    public AbstractContext doReceiveMessage(String message, boolean needSetCheckPoint, String queueId, String offset) {
        if (this.msgIsJsonArray) {
            JSONArray jsonArray = JSONObject.parseArray(message);
            if (jsonArray == null || jsonArray.size() == 0) {
                return null;
            }
            AbstractContext context = null;
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject msgBody = jsonArray.getJSONObject(i);
                boolean checkpoint = false;
                if (needSetCheckPoint && i == jsonArray.size() - 1) {
                    checkpoint = true;
                }
                context = doReceiveMessage(msgBody, checkpoint, queueId, createBatchOffset(offset, i));
                if (!context.isContinue()) {
                    continue;
                }
            }
            return context;
        } else {
            JSONObject jsonObject = create(message);
            return doReceiveMessage(jsonObject, needSetCheckPoint, queueId, offset);
        }
    }

    /**
     * 发送一个系统消息，执行组件不可见，告诉所有组件刷新存储
     *
     * @param queueId
     */
    public void sendCheckpoint(String queueId) {
        Set<String> queues = new HashSet<>();
        queues.add(queueId);
        sendCheckpoint(queues);
    }

    /**
     * 发送系统消息，执行组件不可见，告诉所有组件刷新存储
     *
     * @param queueIds
     */
    public void sendCheckpoint(Set<String> queueIds) {
        JSONObject msg = new JSONObject();
        Message message = createMessage(msg, null, null, true);
        message.getMessageBody().put("_queues", queueIds);
        message.getHeader().setCheckpointQueueIds(queueIds);
        message.getHeader().setNeedFlush(true);
        message.getHeader().setSystemMessage(true);
        if (supportOffsetRest()) {
            message.getHeader().setNeedFlush(false);
        }

        CheckPointMessage checkPointMessage = new CheckPointMessage();
        checkPointMessage.setStreamOperator(this.receiver);
        checkPointMessage.setSource(this);
        message.setSystemMessage(checkPointMessage);
        executeMessage(message);
        if (checkPointMessage.isValidate() && supportOffsetRest()) {
            saveCheckpoint(checkPointMessage);
        }
    }

    protected void saveCheckpoint(CheckPointMessage checkPointMessage) {
        this.checkPointManager.addCheckPointMessage(checkPointMessage);
    }

    public JSONObject createJson(Object message) {
        JSONObject jsonObject = null;
        if (!isJsonData) {
            jsonObject = new UserDefinedMessage(message);
            jsonObject.put(IMessage.DATA_KEY, message);
            jsonObject.put(IMessage.IS_NOT_JSON_MESSAGE, true);
        } else {
            jsonObject = Message.parseObject(message.toString());
        }
        return jsonObject;

    }

    public JSONObject create(String message) {
        return createJson(message);
    }

    /**
     * 交给receiver执行后续逻辑
     *
     * @param channelMessage
     * @return
     */
    public AbstractContext executeMessage(Message channelMessage) {
        AbstractContext context = new Context(channelMessage);
        if (isSplitInRemoving(channelMessage)) {
            return context;
        }
        if (!channelMessage.getHeader().isSystemMessage()) {
            messageQueueChangedCheck(channelMessage.getHeader());
        }

        boolean needFlush = channelMessage.getHeader().isSystemMessage() == false && channelMessage.getHeader().isNeedFlush();

        if (receiver != null) {
            receiver.doMessage(channelMessage, context);
        }
        if (needFlush) {
            sendCheckpoint(channelMessage.getHeader().getQueueId());
        }
        executeMessageAfterReceiver(channelMessage, context);
        return context;
    }

    protected boolean isSplitInRemoving(Message channelMessage) {
        return this.checkPointManager.isRemovingSplit(channelMessage.getHeader().getQueueId());
    }

    /**
     * source 能否自动返现新增的分片，如果不支持，系统将会模拟实现
     *
     * @return
     */
    public abstract boolean supportNewSplitFind();

    /**
     * 能否发现分片移走了，如果不支持，系统会模拟实现
     *
     * @return
     */
    public abstract boolean supportRemoveSplitFind();

    /**
     * 是否运行中，在分片发现时，自动设置分片的offset。必须支持supportNewSplitFind
     *
     * @return
     */
    public abstract boolean supportOffsetRest();

    /**
     * 系统模拟新分片发现，把消息中的分片保存下来，如果第一次收到，认为是新分片
     *
     * @param header
     */
    protected void messageQueueChangedCheck(MessageHeader header) {
        if (supportNewSplitFind() && supportRemoveSplitFind()) {
            return;
        }
        Set<String> queueIds = new HashSet<>();
        String msgQueueId = header.getQueueId();
        if (StringUtil.isNotEmpty(msgQueueId)) {
            queueIds.add(msgQueueId);
        }
        Set<String> checkpointQueueIds = header.getCheckpointQueueIds();
        if (checkpointQueueIds != null) {
            queueIds.addAll(checkpointQueueIds);
        }
        Set<String> newQueueIds = new HashSet<>();
        Set<String> removeQueueIds = new HashSet<>();
        for (String queueId : queueIds) {
            if (isNotDataSplit(queueId)) {
                continue;
            }
            if (StringUtil.isNotEmpty(queueId)) {
                if (!this.checkPointManager.contains(queueId)) {
                    synchronized (this) {
                        if (!this.checkPointManager.contains(queueId)) {
                            this.checkPointManager.addSplit(queueId);
                            newQueueIds.add(queueId);
                        }
                    }
                } else {
                    this.checkPointManager.updateLastUpdate(queueId);
                }
            }
        }
        if (!supportNewSplitFind()) {
            addNewSplit(newQueueIds);
        }

    }

    protected abstract boolean isNotDataSplit(String queueId);

    /**
     * 当分片被移走前需要做的回调
     *
     * @param splitIds 要移走的分片
     */
    public void removeSplit(Set<String> splitIds) {
        if (splitIds == null || splitIds.size() == 0) {
            return;
        }
        this.checkPointManager.addRemovingSplit(splitIds);
        sendRemoveSplitSystemMessage(splitIds);
        //先保存所有的分片
        sendCheckpoint(splitIds);
        this.checkPointManager.flush();
        synchronized (this) {
            for (String splitId : splitIds) {
                this.checkPointManager.removeSplit(splitId);
            }

        }
    }

    public List<ISplit> getAllSplits() {
        return null;
    }

    public Map<String, List<ISplit>> getWorkingSplitsGroupByInstances() {
        return null;
    }

    /**
     * 当新增分片时，需要做的回调
     */
    public void addNewSplit(Set<String> splitIds) {
        if (splitIds == null || splitIds.size() == 0) {
            return;
        }
        this.checkPointManager.deleteRemovingSplit(splitIds);

        JSONObject msg = new JSONObject();
        Message message = createMessage(msg, null, null, false);
        message.getMessageBody().put("_queues", splitIds);
        //message.getHeader().setCheckpointQueueIds(queueIds);

        message.getHeader().setNeedFlush(false);
        message.getHeader().setSystemMessage(true);
        NewSplitMessage systemMessage = new NewSplitMessage(splitIds, this.checkPointManager.getCurrentSplits());
        systemMessage.setStreamOperator(this.receiver);
        systemMessage.setSource(this);
        message.setSystemMessage(systemMessage);
        executeMessage(message);
    }

    /**
     * 发送系统消息，执行组件不可见，告诉所有组件刷新存储
     *
     * @param queueIds
     */
    public void sendRemoveSplitSystemMessage(Set<String> queueIds) {
        JSONObject msg = new JSONObject();
        Message message = createMessage(msg, null, null, true);
        message.getMessageBody().put("_queues", queueIds);
        //message.getHeader().setCheckpointQueueIds(queueIds);
        message.getHeader().setNeedFlush(true);
        message.getHeader().setSystemMessage(true);
        Set<String> currentSplitIds = new HashSet<>();
        currentSplitIds.addAll(this.checkPointManager.getCurrentSplits());
        for (String queueId : queueIds) {
            currentSplitIds.remove(queueId);
        }
        RemoveSplitMessage systemMessage = new RemoveSplitMessage(queueIds, currentSplitIds);
        systemMessage.setStreamOperator(this.receiver);
        systemMessage.setSource(this);
        message.setSystemMessage(systemMessage);
        executeMessage(message);
    }

    /**
     * 如果存在offset，做更新，这里的offset是批流的offset，有系统创建和保存，多用于数据库查询结果场景
     *
     * @param channelMessage
     * @param context
     */
    protected void executeMessageAfterReceiver(Message channelMessage, AbstractContext context) {
        //如果有进度，则保存进度
        if (channelMessage.getHeader() != null && channelMessage.getHeader().getProgress() != null) {
            JSONObject msg = channelMessage.getHeader().getProgress().getCurrentMsg();
            Iterator<Entry<String, Object>> it = msg.entrySet().iterator();
            JSONObject newMsg = new JSONObject();
            newMsg.putAll(msg);
            while (it.hasNext()) {
                Entry<String, Object> entry = it.next();
                String key = entry.getKey();
                if (channelMessage.getMessageBody().containsKey(key)) {
                    newMsg.put(key, channelMessage.getMessageBody().get(key));
                }
            }
            channelMessage.getHeader().getProgress().setCurrentMessage(newMsg.toJSONString());
            channelMessage.getHeader().getProgress().update();
        }

    }

    /**
     * 把json 转换成一个message对象
     *
     * @param msg
     * @return
     */
    public Message createMessage(JSONObject msg, String queueId, String offset, boolean checkpoint) {
        Message channelMessage = new Message(msg);
        channelMessage.getHeader().setSource(this);
        channelMessage.getHeader().setOffset(offset);
        channelMessage.getHeader().setQueueId(queueId);
        channelMessage.getHeader().setNeedFlush(checkpoint);
        channelMessage.setJsonMessage(isJsonData);
        return channelMessage;
    }

    /**
     * 每批次通过加小序号来区分offset的大小
     * @param offset
     * @param i
     * @return
     */
    private String createBatchOffset(String offset, int i) {
        String index = "" + i;
        for (int j = index.length(); j < 5; j++) {
            index = "0" + index;
        }
        return offset + index;
    }

    @Override
    public void setMaxFetchLogGroupSize(int size) {
        this.maxFetchLogGroupSize = size;
    }

    @Override
    public AbstractSource createStageChain(PipelineBuilder pipelineBuilder) {
        return this;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override
    public String getGroupName() {
        return groupName;
    }

    @Override
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override
    public int getMaxThread() {
        return maxThread;
    }

    @Override
    public void setMaxThread(int maxThread) {
        this.maxThread = maxThread;
    }

    public IStreamOperator getReceiver() {
        return receiver;
    }

    public void setReceiver(IStreamOperator receiver) {
        this.receiver = receiver;
    }

    public Boolean getJsonData() {
        return isJsonData;
    }

    public void setJsonData(Boolean jsonData) {
        isJsonData = jsonData;
    }

    public Boolean getMsgIsJsonArray() {
        return msgIsJsonArray;
    }

    public void setMsgIsJsonArray(Boolean msgIsJsonArray) {
        this.msgIsJsonArray = msgIsJsonArray;
    }

    public void setBatchMessage(boolean batchMessage) {
        isBatchMessage = batchMessage;
    }

    public int getMaxFetchLogGroupSize() {
        return maxFetchLogGroupSize;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setCheckpointTime(long checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    public List<String> getLogFingerprintFields() {
        return logFingerprintFields;
    }

    public void setLogFingerprintFields(List<String> logFingerprintFields) {
        this.logFingerprintFields = logFingerprintFields;
    }

    @Override
    public long getCheckpointTime() {
        return checkpointTime;
    }

    public boolean isBatchMessage() {
        return isBatchMessage;
    }

    @Override
    public String createCheckPointName(){

        ISource source = this;

        String namespace = source.getNameSpace();
        String name = source.getConfigureName();
        String groupName = source.getGroupName();


        if(StringUtil.isEmpty(namespace)){
            namespace = "default_namespace";
        }

        if(StringUtil.isEmpty(name)){
            name = "default_name";
        }

        if(StringUtil.isEmpty(groupName)){
            groupName = "default_groupName";
        }
        String topic = source.getTopic();
        if(topic == null || topic.trim().length() == 0){
            topic = "default_topic";
        }
        return MapKeyUtil.createKey(namespace, groupName, topic, name);

    }

    @Override
    public boolean isFinished(){
        return false;
    }

    @Override
    public void finish(){
        checkPointManager.finish();
    }

}
