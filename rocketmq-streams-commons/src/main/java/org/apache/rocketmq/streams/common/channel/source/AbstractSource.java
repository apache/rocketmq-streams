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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointManager;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.context.UserDefinedMessage;
import org.apache.rocketmq.streams.common.interfaces.ILifeCycle;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.metric.SourceMetric;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * channel的抽象，实现了消息的封装，发送等核心逻辑
 */
public abstract class AbstractSource extends BasedConfigurable implements ISource<AbstractSource>, ILifeCycle {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractSource.class);

    public static String CHARSET = "UTF-8";
    /**
     * 输入的消息是否为json
     */
    protected Boolean isJsonData = true;
    /**
     * 输入的消息是否为json array
     */
    protected Boolean msgIsJsonArray = false;

    @ENVDependence protected String groupName;

    protected int maxThread = 2;

    @ENVDependence protected String topic = "";
    /**
     * 多长时间做一次checkpoint
     */
    protected long checkpointTime = 1000 * 30;

    /**
     * 每次拉取的最大条数，多用于消息队列
     */
    protected int maxFetchLogGroupSize = 100;

    /**
     * log fingerprint to filter msg quickly
     */
    protected List<String> logFingerprintFields;
    /**
     * 字节编码方式
     */
    protected String encoding = CHARSET;
    /**
     * 如果是分割符分割，分割符
     */
    protected String fieldDelimiter;
    /**
     * 主要用于分割符拆分字段当场景
     */
    protected MetaData metaData;

    /**
     * 需要数据源支持
     */
    protected List<String> headerFieldNames;

    protected String sendLogTimeFieldName;//可以配置日志中，把日志写入消息队列的时间字段，通过这个字段可以判断队列有无延迟
    protected transient Map<String, SplitProgress> splitProgressMap = new HashMap<>();//每个分片的执行进度
    protected transient Map<String, Long> split2MaxTime = new HashMap<>();//每个分片收到的最大发送时间
    /**
     * 数据源投递消息的算子，此算子用来接收source的数据，做处理
     */
    protected volatile transient IStreamOperator receiver;

    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);

    protected transient Long msgLastReceiverTime = null;

    protected transient SourceMetric sourceMetric = new SourceMetric();

    /**
     * 做checkpoint的管理
     */
    protected transient CheckPointManager checkPointManager = null;

    public AbstractSource() {
        setType(ISource.TYPE);
    }

    @Override protected boolean initConfigurable() {
        hasStart = new AtomicBoolean(false);
        checkPointManager = new CheckPointManager();
        sourceMetric.setMetaData(this.metaData);
        sourceMetric.setSource(this);
        sourceMetric.setMetaData(metaData);
        return super.initConfigurable();
    }

    @Override public boolean start(IStreamOperator receiver) {
        this.receiver = receiver;
        boolean isStartSuccess = false;
        try {
            if (hasStart.compareAndSet(false, true)) {
                isStartSuccess = startSource();
                sourceMetric.setStartTime(System.currentTimeMillis());

            }
            LOGGER.info("[{}][{}] Source_Start_Success", IdUtil.instanceId(), this.getName());
        } catch (Exception e) {
            hasStart.set(false);
            LOGGER.error("[{}][{}] Source_Start_Error", IdUtil.instanceId(), this.getName(), e);
            throw new RuntimeException("Source_Start_Error", e);
        }
        return isStartSuccess;
    }

    @Override public void destroy() {
        try {
            hasStart.set(false);
            destroySource();
            LOGGER.info("[{}][{}] Source_Stop_Success", IdUtil.instanceId(), this.getName());
        } catch (Exception e) {
            hasStart.set(true);
            LOGGER.error("[{}][{}] Source_Stop_Error", IdUtil.instanceId(), this.getName(), e);
            throw new RuntimeException("Source_Stop_Error", e);
        }
    }

    /**
     * 启动 source
     */
    protected abstract boolean startSource();

    protected abstract void destroySource();

    /**
     * 处理消息，并且判断是否需要进行加入check point表识别
     *
     * @param message
     * @param needFlushState 是否需要处理的算子刷新状态
     * @return
     */
    public AbstractContext<?> doReceiveMessage(String message, boolean needFlushState, String queueId, String offset) {
        return doReceiveMessage(message, needFlushState, queueId, offset, new HashMap<>());
    }

    /**
     * 处理消息，并且判断是否需要进行加入check point表识别
     *
     * @param message
     * @param needFlushState 是否需要处理的算子刷新状态
     * @return
     */
    public AbstractContext<?> doReceiveMessage(String message, boolean needFlushState, String queueId, String offset, Map<String, Object> additionValues) {
        if (this.msgIsJsonArray) {
            JSONArray jsonArray = JSONObject.parseArray(message);
            if (jsonArray == null || jsonArray.isEmpty()) {
                return null;
            }
            AbstractContext<?> context = null;
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject msgBody = jsonArray.getJSONObject(i);
                if (CollectionUtil.isNotEmpty(additionValues)) {
                    msgBody.putAll(additionValues);
                }
                boolean checkpoint = needFlushState && i == jsonArray.size() - 1;
                context = doReceiveMessage(msgBody, checkpoint, queueId, createBatchOffset(offset, i));
            }
            return context;
        } else {
            JSONObject jsonObject = create(message);
            if (CollectionUtil.isNotEmpty(additionValues)) {
                jsonObject.putAll(additionValues);
            }
            return doReceiveMessage(jsonObject, needFlushState, queueId, offset);
        }
    }

    /**
     * 提供单条消息的处理逻辑，默认不会加入checkpoint
     *
     * @param message
     * @return
     */
    public AbstractContext<?> doReceiveMessage(JSONObject message, boolean needFlushState, String queueId, String offset) {
        Message msg = createMessage(message, queueId, offset, needFlushState);
        return executeMessage(msg);
    }

    /**
     * 发送一个系统消息，执行组件不可见，告诉所有组件刷新状态
     *
     * @param queueId
     */
    public void sendCheckpoint(String queueId) {
        sendCheckpoint(queueId, null);
    }

    public void sendCheckpoint(String queueId, MessageOffset offset) {
        Set<String> queueIds = new HashSet<>();
        queueIds.add(queueId);
        if (offset != null) {
            CheckPointState checkPointState = new CheckPointState();
            checkPointState.getQueueIdAndOffset().put(queueId, offset);
            sendCheckpoint(queueIds, checkPointState);
        } else {
            sendCheckpoint(queueIds);
        }
    }

    /**
     * 发送系统消息，执行组件不可见，告诉所有组件刷新存储
     *
     * @param queueIds
     */
    public void sendCheckpoint(Set<String> queueIds, CheckPointState... checkPointStates) {
        JSONObject msg = new JSONObject();
        Message message = createMessage(msg, null, null, true);
        message.getMessageBody().put("_queues", queueIds);
        message.getHeader().setCheckpointQueueIds(queueIds);
        message.getHeader().setNeedFlush(true);
        message.getHeader().setSystemMessage(true);

        CheckPointMessage checkPointMessage = new CheckPointMessage();
        if (checkPointStates != null) {
            for (CheckPointState checkPointState : checkPointStates) {
                checkPointMessage.getCheckPointStates().add(checkPointState);
            }
        }
        checkPointMessage.setStreamOperator(this.receiver);
        checkPointMessage.setSource(this);
        message.setSystemMessage(checkPointMessage);
        executeMessage(message);
        sourceMetric.setSplitProgresses(this.getSplitProgress());
        if (checkPointMessage.isValidate()) {
            saveCheckpoint(checkPointMessage);
        }
        LOGGER.info("[{}][{}] Source_Heartbeat_On({})_At({})_LastMsgReceiveTime({})", IdUtil.instanceId(), getName(), String.join(",", queueIds), System.currentTimeMillis(), this.msgLastReceiverTime == null ? "-" : DateUtil.longToString(this.msgLastReceiverTime));
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

    public JSONObject create(byte[] msg, Map<String, ?> headProperties) {
        try {
            String data = new String(msg, getEncoding());
            return create(data, headProperties);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new RuntimeException("msg encode error ");
        }

    }

    public JSONObject create(String message, Map<String, ?> headProperties) {
        JSONObject msg = create(message);
        if (this.headerFieldNames != null && headProperties != null) {
            for (String fieldName : this.headerFieldNames) {
                msg.put(fieldName, headProperties.get(fieldName));
            }
        }
        return msg;
    }

    public JSONObject create(String message) {
        if (isJsonData) {
            return createJson(message);
        }
        //主要是sql场景
        if (this.metaData != null) {
            JSONObject msg = new JSONObject();
            //分割符
            if (this.fieldDelimiter != null) {
                String[] values = message.split(this.fieldDelimiter);

                List<MetaDataField<?>> fields = this.metaData.getMetaDataFields();
                if (values.length != this.metaData.getMetaDataFields().size()) {
                    throw new RuntimeException("expect table column's count equals data size (" + fields.size() + "," + values.length + ")");
                }
                for (int i = 0; i < values.length; i++) {
                    MetaDataField<?> field = fields.get(i);
                    String fieldName = field.getFieldName();
                    String valueStr = values[i];
                    Object value = field.getDataType().getData(valueStr);
                    msg.put(fieldName, value);
                }
                return msg;
            } else {
                //单字段场景
                List<MetaDataField<?>> metaDataFields = this.metaData.getMetaDataFields();
                MetaDataField<?> metaDataField = null;
                for (MetaDataField<?> field : metaDataFields) {
                    if (this.headerFieldNames == null) {
                        metaDataField = field;
                        break;
                    }
                    if (!this.headerFieldNames.contains(field.getFieldName())) {
                        metaDataField = field;
                        break;
                    }
                }
                if (metaDataField != null) {
                    msg.put(metaDataField.getFieldName(), message);
                    return msg;
                }
            }
            return msg;
        } else {
            //sdk场景
            if (this.fieldDelimiter != null) {
                String[] values = message.split(this.fieldDelimiter);
                return createJson(Arrays.asList(values));
            } else {
                return createJson(message);
            }
        }

    }

    public AbstractContext<?> executeMessage(Message channelMessage) {
        AbstractContext<?> context = new Context(channelMessage);
        return executeMessage(channelMessage, context);
    }

    /**
     * 交给receiver执行后续逻辑
     *
     * @param channelMessage
     * @return
     */
    public AbstractContext<?> executeMessage(Message channelMessage, AbstractContext<?> context) {
        if (BatchFinishMessage.isBatchFinishMessage(channelMessage)) {
            /*
             * 系统消息：告诉系统后面没有消息了，需要尽快完成计算，尤其是窗口，不必等窗口触发，可以立即触发窗口
             */
            channelMessage.getHeader().setSystemMessage(true);
            channelMessage.setSystemMessage(new BatchFinishMessage(channelMessage));
        }

        if (isSplitInRemoving(channelMessage)) {
            return context;
        }
//        if (!channelMessage.getHeader().isSystemMessage()) {
//            messageQueueChangedCheck(channelMessage.getHeader());
//        }

        boolean needFlushState = !channelMessage.getHeader().isSystemMessage() && channelMessage.getHeader().isNeedFlush();

        if (receiver != null) {
            try {
                this.msgLastReceiverTime = System.currentTimeMillis();

                if (this.sendLogTimeFieldName != null && channelMessage.getMessageBody().containsKey(this.sendLogTimeFieldName)) {
                    updateSplitProgress(channelMessage.getHeader().getQueueId(), channelMessage.getMessageBody().get(this.sendLogTimeFieldName));
                }
                sourceMetric.setMsgReceivTime(System.currentTimeMillis());
                long start = System.currentTimeMillis();
                receiver.doMessage(channelMessage, context);
                sourceMetric.endCalculate(start);
            } catch (Exception e) {
                LOGGER.error("[{}][{}] Source_SendMsg_Error_On({})_Msg({})_ErrorMsg({})", IdUtil.instanceId(), NameCreator.getFirstPrefix(getName(), IWindow.TYPE, ISource.TYPE), this.getClass().getName(), channelMessage.getMessageBody(), e.getMessage(), e);
            }

        }
        if (needFlushState) {
            sendCheckpoint(channelMessage.getHeader().getQueueId());
        }
        return context;
    }

    protected void updateSplitProgress(String splitId, Object sendLogTime) {
        if (sendLogTime == null) {
            return;
        }
        Long sendLogTimeLong = convertTime2Long(sendLogTime);
        if (sendLogTimeLong == null) {
            return;
        }
        Long maxSendLogTime = this.split2MaxTime.get(splitId);
        if (maxSendLogTime == null || sendLogTimeLong > maxSendLogTime) {
            this.split2MaxTime.put(splitId, sendLogTimeLong);
            this.splitProgressMap.put(splitId, new SplitProgress(splitId, System.currentTimeMillis() - sendLogTimeLong, true));
        }
    }

    /**
     * 如果时间是long，直接返回，如果是string，判断是否是标准格式，如果是转换成long返回
     *
     * @param time
     * @return
     */
    protected Long convertTime2Long(Object time) {
        if (time == null) {
            return null;
        }
        Long timeStamp = null;
        String LONG = "^[-\\+]?[\\d]*$";            // 整数
        if (time instanceof Long) {
            timeStamp = (Long) time;
        } else if (time instanceof String) {
            String timeStr = (String) time;
            if (StringUtil.matchRegex(timeStr, LONG)) {
                timeStamp = Long.valueOf(timeStr);
            } else {
                Date date = DateUtil.parse(timeStr);
                if (date == null) {
                    LOGGER.warn(timeStr + " can not match date format, expect 2022-09-03 12:12:12");
                    return null;
                }
                timeStamp = date.getTime();
            }
        } else {
            LOGGER.warn(time.toString() + " can not match date format, expect 2022-09-03 12:12:12");
            return null;
        }
        if (timeStamp != null) {
            int length = (timeStamp + "").length();
            if (length < 13) {
                int x = 1;
                for (int i = 0; i < 13 - length; i++) {
                    x = x * 10;
                }
                timeStamp = timeStamp * x;
            }
        }
        return timeStamp;
    }

    protected boolean isSplitInRemoving(Message channelMessage) {
        return this.checkPointManager.isRemovingSplit(channelMessage.getHeader().getQueueId());
    }

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
        for (String splitId : splitIds) {
            this.checkPointManager.removeSplit(splitId);
        }

    }

    public Map<String, List<ISplit<?, ?>>> getWorkingSplitsGroupByInstances() {
        return new HashMap<>();
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
     * @param queueIds 队列
     */
    public void sendRemoveSplitSystemMessage(Set<String> queueIds) {
        JSONObject msg = new JSONObject();
        Message message = createMessage(msg, null, null, true);
        message.getMessageBody().put("_queues", queueIds);
        //message.getHeader().setCheckpointQueueIds(queueIds);
        message.getHeader().setNeedFlush(true);
        message.getHeader().setSystemMessage(true);
        Set<String> currentSplitIds = new HashSet<>(this.checkPointManager.getCurrentSplits());
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
     * 把json 转换成一个message对象
     *
     * @param msg 获取的消息
     * @return 返回dipper的message 实例
     */
    public Message createMessage(JSONObject msg, String queueId, String offset, boolean checkpoint) {
        Message channelMessage = new Message(msg);
        channelMessage.getHeader().setSource(this);
        channelMessage.getHeader().setOffset(offset);
        channelMessage.getHeader().setQueueId(queueId);
        channelMessage.getHeader().setNeedFlush(checkpoint);
        channelMessage.getHeader().setPipelineName(getName()); //消息的header中存储Pipeline的名称
        channelMessage.setJsonMessage(isJsonData);
        return channelMessage;
    }

    /**
     * 每批次通过加小序号来区分offset的大小
     *
     * @param offset offset
     * @param i      序号
     * @return offset字符串
     */
    private String createBatchOffset(String offset, int i) {
        StringBuilder index = new StringBuilder("" + i);
        for (int j = index.length(); j < 5; j++) {
            index.insert(0, "0");
        }
        return offset + index;
    }

    @Override public AbstractSource createStageChain(PipelineBuilder pipelineBuilder) {
        return this;
    }

    @Override public void addConfigurables(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
    }

    @Override public String getGroupName() {
        return groupName;
    }

    @Override public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    @Override public int getMaxThread() {
        return maxThread;
    }

    @Override public void setMaxThread(int maxThread) {
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

    public int getMaxFetchLogGroupSize() {
        return maxFetchLogGroupSize;
    }

    @Override public void setMaxFetchLogGroupSize(int size) {
        this.maxFetchLogGroupSize = size;
    }

    @Override public String getTopic() {
        return topic;
    }

    @Override public void setTopic(String topic) {
        this.topic = topic;
    }

    public List<String> getLogFingerprintFields() {
        return logFingerprintFields;
    }

    public void setLogFingerprintFields(List<String> logFingerprintFields) {
        this.logFingerprintFields = logFingerprintFields;
    }

    @Override public long getCheckpointTime() {
        return checkpointTime;
    }

    public void setCheckpointTime(long checkpointTime) {
        this.checkpointTime = checkpointTime;
    }

    @Override public String createCheckPointName() {

        ISource<?> source = this;

        String namespace = source.getNameSpace();
        String name = source.getName();
        String groupName = source.getGroupName();

        if (StringUtil.isEmpty(namespace)) {
            namespace = "default_namespace";
        }

        if (StringUtil.isEmpty(name)) {
            name = "default_name";
        }

        if (StringUtil.isEmpty(groupName)) {
            groupName = "default_groupName";
        }
        String topic = source.getTopic();
        if (topic == null || topic.trim().isEmpty()) {
            topic = "default_topic";
        }
        return MapKeyUtil.createKey(namespace, groupName, topic, name);

    }

    @Override public Map<String, SplitProgress> getSplitProgress() {
        return this.splitProgressMap;
    }

    @Override public boolean isFinished() {
        return false;
    }

    @Override public void finish() {
        checkPointManager.finish();
    }

    public String getEncoding() {
        return encoding;
    }

    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    public String getFieldDelimiter() {
        return fieldDelimiter;
    }

    public void setFieldDelimiter(String fieldDelimiter) {
        this.fieldDelimiter = fieldDelimiter;
    }

    public MetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(MetaData metaData) {
        this.metaData = metaData;
    }

    public List<String> getHeaderFieldNames() {
        return headerFieldNames;
    }

    public void setHeaderFieldNames(List<String> headerFieldNames) {
        this.headerFieldNames = headerFieldNames;
    }

    public String getSendLogTimeFieldName() {
        return sendLogTimeFieldName;
    }

    public void setSendLogTimeFieldName(String sendLogTimeFieldName) {
        this.sendLogTimeFieldName = sendLogTimeFieldName;
    }

    public SourceMetric getSourceMetric() {
        return sourceMetric;
    }

}
