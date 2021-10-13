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

import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

import java.util.*;
import java.util.Map.Entry;

public class CheckPointManager extends BasedConfigurable {

    protected transient Map<String, Long> currentSplitAndLastUpdateTime = new HashMap<>();//保存这个实例处理的分片数

    protected transient Map<String, Long> removingSplits = new HashMap<>();//正在删除的分片

    protected transient ICheckPointStorage iCheckPointStorage;

    public CheckPointManager(){
        String name = ComponentCreator.getProperties().getProperty(ConfigureFileKey.CHECKPOINT_STORAGE_NAME);
        iCheckPointStorage = CheckPointStorageFactory.getInstance().getStorage(name);
    }


    public synchronized void addSplit(String splitId){
        this.currentSplitAndLastUpdateTime.put(splitId,System.currentTimeMillis());
    }
    public synchronized void removeSplit(String splitId){
        this.currentSplitAndLastUpdateTime.remove(splitId);
    }

    public boolean contains(String splitId){
        return this.currentSplitAndLastUpdateTime.containsKey(splitId);
    }

    private final List<CheckPoint> fromSourceState(Map<String, SourceState> sourceStateMap){

        List<CheckPoint> checkPoints = new ArrayList<>();

        for(Entry<String, SourceState> entry : sourceStateMap.entrySet()){
            String key = entry.getKey();
            SourceState value = entry.getValue();
            String[] ss = key.split("\\;");
            assert ss.length == 3 : "key length must be three. format is namespace;pipelineName;sourceName" + key;
            for(Entry<String, MessageOffset> tmpEntry : value.getQueueId2Offsets().entrySet()){
                String queueId = tmpEntry.getKey();
                String offset = tmpEntry.getValue().getMainOffset();
                CheckPoint checkPoint = new CheckPoint();
                checkPoint.setSourceNamespace(ss[0]);
                checkPoint.setPipelineName(ss[1]);
                checkPoint.setSourceName(ss[2]);
                checkPoint.setQueueId(queueId);
                checkPoint.setData(offset);
                checkPoints.add(checkPoint);
            }
        }

        return checkPoints;

    }

    public void addCheckPointMessage(CheckPointMessage message){
        this.iCheckPointStorage.addCheckPointMessage(message);
    }

    public CheckPoint recover(ISource iSource, ISplit iSplit){
        String isRecover =  ComponentCreator.getProperties().getProperty(ConfigureFileKey.IS_RECOVER_MODE);
        if(isRecover != null && Boolean.valueOf(isRecover)){
            String queueId = iSplit.getQueueId();
            return iCheckPointStorage.recover(iSource, queueId);
        }
        return null;
    }


    public void updateLastUpdate(String queueId) {
        addSplit(queueId);
    }

    public Set<String> getCurrentSplits() {
        return this.currentSplitAndLastUpdateTime.keySet();
    }

    public void flush(){
        iCheckPointStorage.flush();
    }




    /**
     * 根据source进行划分，主要是针对双流join的场景
     * @param source
     * @return
     */
    public static String createSourceName(ISource source, String pipelineName){

        if(StringUtil.isNotEmpty(pipelineName)){
            return MapKeyUtil.createKey(source.createCheckPointName(), pipelineName);
        }
        if(source==null){
            return null;
        }
        return source.createCheckPointName();
    }




    public Map<String, Long> getCurrentSplitAndLastUpdateTime() {
        return currentSplitAndLastUpdateTime;
    }

    public synchronized void addRemovingSplit(Set<String> removingSplits) {
        long removingTime = System.currentTimeMillis();
        for (String split : removingSplits) {
            this.removingSplits.put(split, removingTime);
        }
    }

    public synchronized void deleteRemovingSplit(Set<String> removingSplits) {
        for (String split : removingSplits) {
            this.removingSplits.remove(split);
        }

    }

    public synchronized boolean isRemovingSplit(String splitId) {
        Long removingTime = this.removingSplits.get(splitId);
        if (removingTime == null) {
            return false;
        }
        //超过10秒才允许当作新分片进来
        if (System.currentTimeMillis() - removingTime > 10 * 1000) {
            this.removingSplits.remove(splitId);
            return false;
        }
        return true;
    }

    public static final String createCheckPointKey(String key, String queueId){
        return key + "^^^" + queueId;
    }

    public static final String[] parseCheckPointKey(String checkPointKey){
        return checkPointKey.split("\\^\\^\\^");
    }

    public static final String getNameSpaceFromCheckPointKey(String checkPointKey){
        return parseCheckPointKey(checkPointKey)[0].split("\\;")[0];
    }

    public static final String getGroupNameFromCheckPointKey(String checkPointKey){
        return parseCheckPointKey(checkPointKey)[0].split("\\;")[1];
    }

    public static final String getNameFromCheckPointKey(String checkPointKey){
        return parseCheckPointKey(checkPointKey)[0].split("\\;")[2];
    }

    public static final String getTopicFromCheckPointKey(String checkPointKey){
        return parseCheckPointKey(checkPointKey)[0].split("\\;")[3];
    }

    public static final String getQueueIdFromCheckPointKey(String checkPointKey){
        return parseCheckPointKey(checkPointKey)[1];
    }

    public static void main(String[] args){
        SourceSnapShot snapShot = new SourceSnapShot();
        snapShot.setId(1L);
        snapShot.setGmtCreate(new Date());
        snapShot.setGmtModified(new Date());
        snapShot.setKey("key");
        snapShot.setValue("value");
        System.out.println(ReflectUtil.serializeObject(snapShot));

    }
}
