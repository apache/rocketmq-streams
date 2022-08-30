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
package org.apache.rocketmq.streams.common.context;

import com.alibaba.fastjson.JSONObject;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.common.optimization.MessageGlobleTrace;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 每个消息有个头部，代表消息的一些支持信息
 */
public class MessageHeader {

    public static final String JOIN_LEFT = "left";
    public static final String JOIN_RIGHT = "right";

    /**
     * 因为是字符串比较，需要有一个固定位数
     */
    public static final int SPLIT_OFFST_INIT = 10000000;
    protected String pipelineName;

    /**
     * 当前消息的channel信息
     */
    private transient ISource source;
    /**
     * 路由用到的路由标签，标签的值是stage的label，用于路由stage，可以多个label
     */
    private String routeLabels;
    /**
     * 路由用到的路由标签，标签的值是stage的label，用于路由stage,主要用于过滤，可以多个label
     */
    private String filterLabels;
    /**
     * 消息所属的queue id
     */
    private String queueId = "1";
    /**
     * 消息的offset，代表整个消息的offset
     */
    private MessageOffset messageOffset = new MessageOffset();//保存消息的offset

    private ISplit messageQueue;

    /**
     * 消息发送时间
     */
    private long sendTime = System.currentTimeMillis();
    /**
     * 是否需要刷新数据
     */
    private boolean needFlush;
    /**
     * 当前进度，保存当前进度
     */
    private BatchMessageOffset progress;
    /**
     * 是否是系统消息
     */
    private boolean isSystemMessage = false;

    protected Set<String> checkpointQueueIds;//当是系统消息时，保存checkpoint信息
    /**
     * 在循环模式使用，主要表示当前的循环index
     */
    private int loopIndex = -1;

    /**
     * 在pipline中消息会被拆分，在有多分支时，会被copy，这个对象会在任何变动时，都保持全局唯一，不允许copy，复制，创建，一个message全局唯一
     */
    protected MessageGlobleTrace messageGlobalTrace;

    /**
     * trace id of every message
     */
    protected String traceId = IMessage.DEFAULT_MESSAGE_TRACE_ID;

    protected String msgRouteFromLable;//消息从哪里来的标签，标记上游节点的标记，主要是通过build table name来标记

    private String originTable;

    protected String logFingerprintValue;//日志指纹的值

    public MessageHeader copy() {
        MessageHeader header = new MessageHeader();
        header.setSource(source);
        header.routeLabels = routeLabels;
        header.filterLabels = filterLabels;
        header.queueId = queueId;
        header.messageOffset = new MessageOffset(messageOffset.getOffsetStr(), messageOffset.isLongOfMainOffset());
        header.sendTime = sendTime;
        header.needFlush = needFlush;
        header.isSystemMessage = isSystemMessage;
        header.progress = new BatchMessageOffset();
        if (progress != null) {
            header.progress.setCurrentMessage(progress.getCurrentMessage());
            header.progress.setOwnerType(progress.getOwnerType());
        }
        header.messageGlobalTrace = messageGlobalTrace;//这里不必复制，会保持全局唯一
        header.traceId = traceId;
        header.msgRouteFromLable = msgRouteFromLable;
        header.logFingerprintValue = logFingerprintValue;
        header.messageQueue = messageQueue;
        header.checkpointQueueIds = checkpointQueueIds;
        return header;
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        ReflectUtil.setFieldValue2Object(this, jsonObject);
        return jsonObject;
    }
    public Set<String> createRouteLableSet(String routeLabels){
        if(routeLabels==null){
            return null;
        }
        String[] lables=MapKeyUtil.splitKey(routeLabels);
        Set<String> routeLableSet=new HashSet<>();
        for(String lable:lables){
            routeLableSet.add(lable);
        }
        return routeLableSet;
    }
    /**
     * 用于路由的标签，标签等于stage的label
     *
     * @param labels
     */
    public String addRouteLabel(String... labels) {
        this.routeLabels = createLables(routeLabels, labels);
        return this.routeLabels;
    }

    /**
     * 用于路由的标签，标签等于stage的label
     */
    public String addFilterLabel(String... labels) {
        this.filterLabels = createLables(filterLabels, labels);
        return this.filterLabels;
    }

    public void setQueueId(String queueId) {
        this.queueId = queueId;
    }

    public ISource getSource() {
        return source;
    }

    public void setSource(ISource source) {
        this.source = source;
    }

    public String getRouteLabels() {
        return routeLabels;
    }


    public String getFilterLabels() {
        return filterLabels;
    }

    public String getQueueId() {
        return queueId;
    }

    public boolean isEmptyOffset() {
        return messageOffset == null;
    }

    public String getOffset() {
        if (messageOffset == null) {
            return null;
        }
        return messageOffset.getOffsetStr();
    }

    public ISplit getMessageQueue() {
        return messageQueue;
    }

    public void setMessageQueue(ISplit messageQueue) {
        this.messageQueue = messageQueue;
    }

    /**
     * 比较当前offset是否比输入的offset大，如果大返回true，否则返回false。 考虑拆分的场景
     *
     * @param dstOffset
     * @return
     */
    public boolean greateThan(String dstOffset) {
        return messageOffset.greateThan(dstOffset);
    }

    public String getTraceId() {
        return traceId;
    }

    public void setTraceId(String traceId) {
        this.traceId = traceId;
    }

    /**
     * 创建路由标签
     *
     * @param routeLabels
     * @param labels
     */
    protected String createLables(String routeLabels, String... labels) {
        if (StringUtil.isEmpty(routeLabels)) {
            routeLabels = MapKeyUtil.createKey(labels);

        } else {
            String tmp = MapKeyUtil.createKey(labels);
            routeLabels = MapKeyUtil.createKey(routeLabels, tmp);
        }
        return routeLabels;
    }



    public boolean isNeedFlush() {
        return needFlush;
    }

    public void setNeedFlush(boolean needFlush) {
        this.needFlush = needFlush;
    }

    public void setRouteLabels(String routeLabels) {
        this.routeLabels = routeLabels;
    }

    public void setFilterLabels(String filterLabels) {
        this.filterLabels = filterLabels;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public long getSendTime() {
        return sendTime;
    }

    public Boolean getOffsetIsLong() {
        return messageOffset.isLongOfMainOffset();
    }

    public void setOffsetIsLong(Boolean offsetIsLong) {
        messageOffset.setLongOfMainOffset(offsetIsLong);
    }

    public void addLayerOffset(long offset) {
        messageOffset.getOffsetLayers().add(offset);
    }

    public MessageOffset getMessageOffset() {
        return messageOffset;
    }

    public boolean isSystemMessage() {
        return isSystemMessage;
    }

    public BatchMessageOffset getProgress() {
        return progress;
    }

    public void setProgress(BatchMessageOffset progress) {
        this.progress = progress;
    }

    public int getLoopIndex() {
        return loopIndex;
    }

    public Set<String> getCheckpointQueueIds() {
        return checkpointQueueIds;
    }

    public void setCheckpointQueueIds(Set<String> checkpointQueueIds) {
        this.checkpointQueueIds = checkpointQueueIds;
    }

    public void setLoopIndex(int loopIndex) {
        this.loopIndex = loopIndex;
    }

    public void setSystemMessage(boolean systemMessage) {
        isSystemMessage = systemMessage;
    }



    public MessageGlobleTrace getMessageGlobalTrace() {
        return messageGlobalTrace;
    }

    public void setMessageGlobalTrace(MessageGlobleTrace messageGlobalTrace) {
        this.messageGlobalTrace = messageGlobalTrace;
    }

    public String getMsgRouteFromLable() {
        return msgRouteFromLable;
    }

    public void setMsgRouteFromLable(String msgRouteFromLable) {
        this.msgRouteFromLable = msgRouteFromLable;
    }

    public String getLogFingerprintValue() {
        return logFingerprintValue;
    }

    public void setLogFingerprintValue(String logFingerprintValue) {
        this.logFingerprintValue = logFingerprintValue;
    }

    public void setOffset(String offset) {
        messageOffset.parseOffsetStr(offset);
    }

    public void setOffset(Integer offset) {
        messageOffset.mainOffset = (offset + "");
        messageOffset.isLongOfMainOffset = true;
    }

    public void setOffset(Long offset) {
        messageOffset.mainOffset = (offset + "");
        messageOffset.isLongOfMainOffset = true;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public void setPipelineName(String pipelineName) {
        this.pipelineName = pipelineName;
    }

    public String getOriginTable() {
        return originTable;
    }

    public void setOriginTable(String originTable) {
        this.originTable = originTable;
    }
}
