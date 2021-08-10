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

import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.window.operator.impl.WindowOperator.WindowRowOperator;
import org.apache.rocketmq.streams.window.source.WindowRireSource;
import org.apache.rocketmq.streams.window.storage.ShufflePartitionManager;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * 负责处理分片
 */
public class ShuffleChannel extends AbstractSystemChannel {

    protected static final Log LOG = LogFactory.getLog(ShuffleChannel.class);

    protected static final String SHUFFLE_QUEUE_ID = "SHUFFLE_QUEUE_ID";
    private static final String SHUFFLE_KEY = "SHUFFLE_KEY";
    private static final String PROCESS_NAME = "PROCESS_NAME";

    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";
    protected String MSG_OWNER = "MSG_OWNER";//消息所属的window

    protected String WINDOW_INSTANCE_START_TIME = "_start_time";
    protected String WINDOW_INSTANCE_END_TIME = "_end_time";

    private static final String SHUFFLE_TRACE_ID = "SHUFFLE_TRACE_ID";

    protected ShuffleOutputDataSource shuffleSink;

    protected Map<String, ISplit> queueMap = new ConcurrentHashMap<>();
    protected List<ISplit> queueList;//所有的分片

    // protected NotifyChannel notfiyChannel;//负责做分片的通知管理
    protected AbstractShuffleWindow window;
    private Set<String> currentQueueIds;//当前管理的分片

    protected transient WindowRireSource windowRireSource;

    public ShuffleChannel(AbstractShuffleWindow window) {
        this.window = window;
        channelConfig = new HashMap<>();
        channelConfig.put(CHANNEL_PROPERTY_KEY_PREFIX, ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX);
        channelConfig.put(CHANNEL_TYPE, ConfigureFileKey.WINDOW_SHUFFLE_CHANNEL_TYPE);
        this.consumer = createSource(window.getNameSpace(),window.getConfigureName());

        this.producer = createSink(window.getNameSpace(),window.getConfigureName());
        if(this.consumer==null||this.producer==null){
            autoCreateShuffleChannel(window.getFireReceiver().getPipeline());
        }
        if(this.consumer instanceof AbstractSource){
            ((AbstractSource)this.consumer).setJsonData(true);
        }
        this.shuffleSink = createWindowTaskOutputDataSource();
        this.shuffleSink.openAutoFlush();
        //this.notfiyChannel = NotifyChannel.getInstance();
        //if(this.notfiyChannel.producer==null||this.notfiyChannel.consumer==null){
        //    this.notfiyChannel.autoCreateShuffleChannel(window.getFireReceiver().getPipeline());
        //}
        //this.notfiyChannel.startChannel();//启动通知管理，里面做了重入，最终只启动一个
        if (producer!=null&&(queueList == null  || queueList.size() == 0) ){
            queueList = producer.getSplitList();
            Map<String, ISplit> tmp = new ConcurrentHashMap<>();
            for (ISplit queue : queueList) {
                tmp.put(queue.getQueueId(), queue);
            }

            this.queueMap = tmp;
        }
    }

    @Override
    protected String getDynamicPropertyValue() {
        String dynamicPropertyValue= MapKeyUtil.createKey(window.getNameSpace(),window.getConfigureName());
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";","_");
        return dynamicPropertyValue;
    }

    @Override
    protected int getShuffleSplitCount(AbstractSupportShuffleSink shuffleSink) {
        int splitNum=shuffleSink.getSplitNum();
        return splitNum>0?splitNum:32;
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
            ISystemMessage systemMessage=oriMessage.getSystemMessage();
            if(systemMessage instanceof CheckPointMessage){
                this.checkpoint(oriMessage, context,(CheckPointMessage)systemMessage);
            }else if(systemMessage instanceof NewSplitMessage){
                this.addNewSplit(oriMessage,context,(NewSplitMessage)systemMessage);
            }else if(systemMessage instanceof RemoveSplitMessage){
                this.removeSplit(oriMessage,context,(RemoveSplitMessage)systemMessage);
            }else {
                throw new RuntimeException("can not support this system message "+systemMessage.getClass().getName());
            }
            afterFlushCallback(oriMessage,context);
            return null;

        }
        /**
         * 过滤不是这个window的消息，一个shuffle通道，可能多个window共享，这里过滤掉非本window的消息
         */
        boolean isFilter=filterNotOwnerMessage(oriMessage);
        if(isFilter){
            return null;
        }
        String queueId=oriMessage.getHeader().getQueueId();
        //ISplit channelQueue=queueMap.get(queueId);
        //boolean containQueueId=notfiyChannel.contains(queueId);
        //notfiyChannel.dealMessageQueue(channelQueue);

        JSONArray messages = oriMessage.getMessageBody().getJSONArray(SHUFFLE_MESSAGES);
        if(messages==null){
            return null;
        }

        String traceId = oriMessage.getMessageBody().getString(SHUFFLE_TRACE_ID);
        if (!StringUtil.isEmpty(traceId)) {
            TraceUtil.debug(traceId, "shuffle message in", "received message size:" + messages.size());
        }

        for (Object obj: messages) {
            IMessage message = new Message((JSONObject) obj);
            message.getHeader().setQueueId(queueId);

            List<WindowInstance> windowInstances=window.queryOrCreateWindowInstance(message,queueId);
            if(windowInstances==null||windowInstances.size()==0){
                continue;
            }
            for(WindowInstance windowInstance:windowInstances){
                String windowInstanceId = windowInstance.createWindowInstanceId();
                if(!window.getWindowInstanceMap().containsKey(windowInstanceId)){
                    window.getWindowInstanceMap().putIfAbsent(windowInstanceId,windowInstance);
                    synchronized (this){
                        if(window.getFireMode()==2){
                            //这个模式窗口触发不会清理数据，需要额外的创建一个实例做最后的存储清理
                            Date endTime=DateUtil.parseTime(windowInstance.getEndTime());
                            Date lastFireTime=DateUtil.addDate(TimeUnit.SECONDS,endTime,window.getWaterMarkMinute()*window.getTimeUnitAdjust());
                            WindowInstance lastClearWindowInstance=window.createWindowInstance(windowInstance.getStartTime(),windowInstance.getEndTime(),DateUtil.format(lastFireTime),queueId);

                            addNeedFlushWindowInstance(lastClearWindowInstance);
                        }
                    }

                }
                if(windowInstance.isNewWindowInstance()){
                    addNeedFlushWindowInstance(windowInstance);
                    windowInstance.setNewWindowInstance(false);
                    ShufflePartitionManager.getInstance().setWindowInstanceFinished(windowInstance.createWindowInstanceId());
                }
            }
            for(WindowInstance windowInstance:windowInstances){
                window.updateMaxEventTime(message);
                window.getWindowFireSource().updateWindowInstanceLastUpdateTime(windowInstance);
            }
            message.getMessageBody().put(WindowInstance.class.getSimpleName(), windowInstances);
            message.getMessageBody().put(AbstractWindow.class.getSimpleName(), window);
            beforeBatchAdd(oriMessage,message);
            shuffleSink.batchAdd(message);

        }

        return null;
    }

    @Override
    protected Map<String, String> getChannelConfig() {
        return channelConfig;
    }

    @Override
    protected void putDynamicPropertyValue(Set<String> dynamiPropertySet,Properties properties){
        String groupName="groupName";
        if(!dynamiPropertySet.contains(groupName)){
            properties.put(groupName,getDynamicPropertyValue());
        }
        if(!dynamiPropertySet.contains("tags")){
            properties.put("tags",getDynamicPropertyValue());
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

    /**
     * 对于接收到新的window task消息后的处理器
     *
     * @return
     */
    protected ShuffleOutputDataSource createWindowTaskOutputDataSource() {

        ShuffleOutputDataSource outputDataSource =new ShuffleOutputDataSource();
        outputDataSource.init();
        outputDataSource.setConfigureName("shuffleChannel");
        return outputDataSource;
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        this.currentQueueIds=newSplitMessage.getCurrentSplitIds();
        List<WindowInstance> allWindowInstances=WindowInstance.queryAllWindowInstance(DateUtil.getCurrentTimeString(),window,newSplitMessage.getSplitIds());
        if(CollectionUtil.isNotEmpty(allWindowInstances)){
            Set<String> partitionNumKeys=new HashSet<>();
            for(WindowInstance windowInstance:allWindowInstances){
                windowInstance.setNewWindowInstance(false);
                window.getWindowInstanceMap().putIfAbsent(windowInstance.createWindowInstanceId(),windowInstance);
                window.getWindowFireSource().registFireWindowInstanceIfNotExist(windowInstance,window);
                for(String queueId:newSplitMessage.getSplitIds()){
                    String key=window.getWindowMaxValueManager().createSplitNumberKey(windowInstance,queueId);
                    partitionNumKeys.add(key);
                    window.getStorage().loadSplitData2Local(queueId,windowInstance.createWindowInstanceId(),window.getWindowBaseValueClass(),new WindowRowOperator(windowInstance,queueId,window));
                }
            }
            window.getWindowMaxValueManager().loadMaxSplitNum(partitionNumKeys);

        }else {
            for(String queueId:newSplitMessage.getSplitIds()){
                ShufflePartitionManager.getInstance().setSplitFinished(queueId);
            }
        }
        window.getFireReceiver().doMessage(message,context);
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        this.currentQueueIds=removeSplitMessage.getCurrentSplitIds();
        Set<String> queueIds=removeSplitMessage.getSplitIds();
        if(queueIds!=null){
            for(String queueId:queueIds){
                ShufflePartitionManager.getInstance().setSplitInValidate(queueId);
                window.clearCache(queueId);

            }
            window.getWindowMaxValueManager().removeKeyPrefixFromLocalCache(queueIds);
            //window.getWindowFireSource().removeSplit(queueIds);
        }
        window.getFireReceiver().doMessage(message,context);
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        if(message.getHeader().isNeedFlush()){
            this.flush(message.getHeader().getCheckpointQueueIds());
        }
        CheckPointState checkPointState=  new CheckPointState();
        checkPointState.setQueueIdAndOffset(this.shuffleSink.getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);
    }

    @Override
    public String getConfigureName() {
        return window.getConfigureName()+"_shuffle";
    }

    @Override
    public String getNameSpace() {
        return window.getNameSpace();
    }

    @Override
    public String getType() {
        return Pipeline.TYPE;
    }

    protected class ShuffleOutputDataSource extends WindowCache {
        protected  List<WindowInstance> notSaveWindowInstances=new ArrayList<>();//未保存的windowinstance

        public ShuffleOutputDataSource() {
        }

        @Override
        protected boolean batchInsert(List<IMessage> messageList) {
            Map<Pair<String, String>, List<IMessage>> instance2Messages = new HashMap<>();
            Map<String, WindowInstance> windowInstanceMap = new HashMap<>();
            groupByWindowInstanceAndQueueId(messageList, instance2Messages, windowInstanceMap);
            Iterator<Map.Entry<Pair<String, String>, List<IMessage>>> it = instance2Messages.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Pair<String, String>, List<IMessage>> entry = it.next();
                Pair<String, String> queueIdAndInstanceKey = entry.getKey();
                List<IMessage> messages = entry.getValue();
                WindowInstance windowInstance = windowInstanceMap.get(queueIdAndInstanceKey.getRight());
                window.shuffleCalculate(messages, windowInstance, queueIdAndInstanceKey.getLeft());
                DebugWriter.getDebugWriter(window.getConfigureName()).writeShuffleCalculate(window,messages,windowInstance);
            }
            return true;
        }

        @Override
        public boolean flushMessage(List<IMessage> messages) {
            saveOtherState();
            return super.flushMessage(messages);
        }

        protected void saveOtherState(){
            if(notSaveWindowInstances.size()>0){
                if(notSaveWindowInstances.size()>0){
                    List<WindowInstance> copy=null;
                    synchronized (this){
                        copy=this.notSaveWindowInstances;
                        this.notSaveWindowInstances=new ArrayList<>();
                    }
                    Set<String> existWindowInstaceIds=new HashSet<>();
                    List<WindowInstance> windowInstances=new ArrayList<>();
                    for(WindowInstance windowInstance:copy){
                        String windowInstanceId=windowInstance.createWindowInstanceId();
                        if(existWindowInstaceIds.contains(windowInstanceId)){
                            continue;
                        }
                        windowInstances.add(windowInstance);
                        existWindowInstaceIds.add(windowInstanceId);
                    }
                    ORMUtil.batchReplaceInto(windowInstances);
                }

            }
            window.getWindowMaxValueManager().flush();
        }

        @Override
        protected String generateShuffleKey(IMessage message) {
            return null;
        }
    }


    /**
     * 根据message，把message分组到不同的group，分别处理
     *
     * @param messageList
     * @param instance2Messages
     * @param windowInstanceMap
     */
    protected void groupByWindowInstanceAndQueueId(List<IMessage> messageList, Map<Pair<String, String>, List<IMessage>> instance2Messages,
        Map<String, WindowInstance> windowInstanceMap) {
        for (IMessage message : messageList) {

            List<WindowInstance> windowInstances = (List<WindowInstance>)message.getMessageBody().get(WindowInstance.class.getSimpleName());
            String queueId = message.getHeader().getQueueId();
            for(WindowInstance windowInstance:windowInstances){
                String windowInstanceId = windowInstance.createWindowInstanceId();
                Pair<String, String> queueIdAndInstanceKey = Pair.of(queueId, windowInstanceId);
                List<IMessage> messages = instance2Messages.get(queueIdAndInstanceKey);
                if (messages == null) {
                    messages = new ArrayList<>();
                    instance2Messages.put(queueIdAndInstanceKey, messages);
                }
                messages.add(message);
                windowInstanceMap.put(windowInstanceId, windowInstance);
            }

            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
            message.getHeader().setQueueId(oriQueueId);
            message.getHeader().setOffset(oriOffset);

        }
    }

    public ISplit getSplit(Integer index){
        return queueList.get(index);
    }

    public JSONObject createMsg(JSONArray messages,ISplit split) {

        JSONObject msg = new JSONObject();

        msg.put(SHUFFLE_QUEUE_ID, split.getQueueId());//分片id
        msg.put(SHUFFLE_MESSAGES, messages);//合并的消息
        msg.put(MSG_OWNER,getDynamicPropertyValue());//消息owner

        StringBuilder traceIds = new StringBuilder();
        for (int i = 0; i < messages.size(); i++) {
            JSONObject object = messages.getJSONObject(i);
            if (object.containsKey(WindowCache.ORIGIN_MESSAGE_TRACE_ID)) {
                traceIds.append(object.getString(WindowCache.ORIGIN_MESSAGE_TRACE_ID)).append(";");
            }
        }
        msg.put(SHUFFLE_TRACE_ID, traceIds);
        TraceUtil.debug(traceIds.toString(), "origin message out", split.getQueueId());

        return msg;
    }

    public ISplit getChannelQueue(String key){
        int index=hash(key);
        ISplit targetQueue = queueList.get(index);
        return targetQueue;
    }

    public  int hash(Object key) {
        int mValue=queueList.size();
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
        shuffleSink.flush(checkpointQueueIds);
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
        String owner=oriMessage.getMessageBody().getString(MSG_OWNER);
        if(owner!=null&&owner.equals(getDynamicPropertyValue())){
            return false;
        }
        return true;
    }

    public Set<String> getCurrentQueueIds() {
        return currentQueueIds;
    }

    public List<ISplit> getQueueList() {
        return queueList;
    }

    public synchronized void addNeedFlushWindowInstance(WindowInstance windowInstance){
        if(!window.isLocalStorageOnly()){
            this.shuffleSink.notSaveWindowInstances.add(windowInstance);
        }
    }

    public synchronized void clearCache(WindowInstance windowInstance){
        this.shuffleSink.notSaveWindowInstances.remove(windowInstance);
    }

    public AbstractShuffleWindow getWindow() {
        return window;
    }
}