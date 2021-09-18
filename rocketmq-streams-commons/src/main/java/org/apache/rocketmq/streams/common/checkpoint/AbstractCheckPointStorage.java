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
package org.apache.rocketmq.streams.common.checkpoint;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.context.MessageOffset;

import java.util.*;

public abstract class AbstractCheckPointStorage implements ICheckPointStorage{

    static final Log logger = LogFactory.getLog(AbstractCheckPointStorage.class);

    protected transient IMessageCache<CheckPointMessage> messageCache;

    public AbstractCheckPointStorage(){
        messageCache = new MessageCache<>(new IMessageFlushCallBack<CheckPointMessage>() {
            @Override
            public boolean flushMessage(List<CheckPointMessage> messages) {
                //合并最近的checkpoint，只存储一次
                // key 为 sourceName, Value中的map， k : v = queueid : offset
                Map<String, SourceState> sourceStateMap = mergeSourceState(messages);

                logger.info(String.format("flushMessage raw size %d, merge size %d", messages.size(), sourceStateMap.size()));
                logger.info("flushMessage : " + messages.get(0).getCheckPointStates().get(0).getQueueIdAndOffset().toString());

                saveCheckPoint(sourceStateMap);
                return true;
            }
        });
        ((MessageCache)messageCache).setAutoFlushSize(10);
        ((MessageCache)messageCache).setAutoFlushTimeGap(500);
        messageCache.openAutoFlush();
    }

    public void flush(){
        messageCache.flush();
    }


    /**
     * 可能有多次的offset合并在一起，对offset合并
     * 合并包含两个逻辑：1.同1个CheckPointMessage中，选择最小的作为本次的offset
     * @param messages
     */
    protected  Map<String, SourceState> mergeSourceState(List<CheckPointMessage> messages) {
        Map<String,SourceState> sourceStateMap = new HashMap<>();
        for(CheckPointMessage checkPointMessage:messages){
            SourceState sourceState = createSourceState(checkPointMessage);
            if(sourceState == null){
                continue;
            }
            String sourceName = sourceState.getSourceName();
            SourceState existSourceState = sourceStateMap.get(sourceName);
            SourceState lastSourceState = sourceState;
            if(existSourceState != null){
                lastSourceState = merge(sourceState,existSourceState);
            }
            sourceStateMap.put(sourceName,lastSourceState);
        }
        return sourceStateMap;
    }

    /**
     * 如果多次的checkpoint在一起，先合并再保存
     * @param sourceState
     * @param existSourceState
     * @return
     */
    protected SourceState merge(SourceState sourceState, SourceState existSourceState) {
        Iterator<Map.Entry<String, MessageOffset>> it = sourceState.getQueueId2Offsets().entrySet()
            .iterator();
        while (it.hasNext()){
            Map.Entry<String, MessageOffset> entry=it.next();
            String queueId = entry.getKey();
            MessageOffset offset = entry.getValue();
            MessageOffset existOffset = existSourceState.getQueueId2Offsets().get(queueId);
            if(existOffset == null){
                existSourceState.getQueueId2Offsets().put(queueId,offset);
            }else {
                boolean isGreateThan = offset.greateThan(existOffset.getOffsetStr());
                if(isGreateThan){
                    existSourceState.getQueueId2Offsets().put(queueId,offset);
                }
            }
        }
        return existSourceState;
    }

    /**
     * 一个pipeline流程中，找最小的offset提交保存
     * @param checkPointMessage
     * @return
     */
    protected SourceState createSourceState(CheckPointMessage checkPointMessage) {
        SourceState sourceState = new SourceState();
        String pipelineName = checkPointMessage.getPipelineName();

        Map<String, MessageOffset> queueId2Offsets = new HashMap<>();
        sourceState.setSourceName(CheckPointManager.createSourceName(checkPointMessage.getSource(),pipelineName));
        sourceState.setQueueId2Offsets(queueId2Offsets);

        for(CheckPointState checkPointState:checkPointMessage.getCheckPointStates()){

            if(checkPointState.isReplyAnyOny()){
                continue;
            }
            if(checkPointState.isReplyRefuse()){
                return null;
            }
            Iterator<Map.Entry<String, MessageOffset>> it = checkPointState.getQueueIdAndOffset().entrySet()
                .iterator();
            while (it.hasNext()){
                Map.Entry<String, MessageOffset> entry=it.next();
                String queueId=entry.getKey();
                MessageOffset offset = entry.getValue();
                MessageOffset existOffset = queueId2Offsets.get(queueId);
                if(existOffset==null){
                    queueId2Offsets.put(queueId,offset);
                }else {
                    boolean isGreateThan=existOffset.greateThan(offset.getOffsetStr());
                    if(isGreateThan){
                        queueId2Offsets.put(queueId,offset);
                    }else {
                        queueId2Offsets.put(queueId,existOffset);
                    }
                }
            }
        }
        return sourceState;
    }

    /**
     * 先查询现在数据源的分片，如果已经不处理的分片，不做保存
     * 否则把结果保存到db中
     * @param sourceStateMap
     */
    protected void saveCheckPoint(Map<String, SourceState> sourceStateMap) {

        List<SourceSnapShot> checkPoints = new ArrayList<>();

        for(SourceState sourceState:sourceStateMap.values()){
            for(Map.Entry<String, MessageOffset> entry : sourceState.getQueueId2Offsets().entrySet()){
                CheckPoint checkPoint = new CheckPoint();
                checkPoint.setSourceName(sourceState.getSourceName());
                checkPoint.setQueueId(entry.getKey());
                checkPoint.setData(entry.getValue().getMainOffset());
                checkPoint.setGmtCreate(new Date());
                checkPoint.setGmtModified(new Date());
                SourceSnapShot object = checkPoint.toSnapShot();
                checkPoints.add(object);

            }
        }
        save(checkPoints);
    }

    public void addCheckPointMessage(CheckPointMessage message){
        List<CheckPointState> states = message.getCheckPointStates();
        for(CheckPointState state : states){
            logger.debug(String.format("addCheckPointMessage states %s", state.getQueueIdAndOffset().toString()));
        }
        messageCache.addCache(message);
    }

}
