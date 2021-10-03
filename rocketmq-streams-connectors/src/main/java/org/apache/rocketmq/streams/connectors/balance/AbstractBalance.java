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
package org.apache.rocketmq.streams.connectors.balance;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.connectors.source.SourceInstance;

public abstract class AbstractBalance implements ISourceBalance {

    protected int balanceCount = 0;

    @Override
    public SplitChanged doBalance(List<ISplit> allSplits, List<ISplit> ownerSplits) {
        balanceCount++;
        heartBeat();
        List<SourceInstance> sourceInstances = fetchSourceInstances();
        List<ISplit> workingSplits = fetchWorkingSplits(allSplits);
        SplitChanged splitChanged = getAdditionSplits(allSplits, sourceInstances, workingSplits, ownerSplits);
        if(splitChanged != null){
            return splitChanged;
        }
        splitChanged = getRemoveSplits(allSplits, sourceInstances, workingSplits, ownerSplits);
        return splitChanged;
    }



    protected void heartBeat() {
        holdLockSourceInstance();
    }

    /**
     * get all dispatch splits
     * @return
     */
    protected abstract List<ISplit> fetchWorkingSplits(List<ISplit> allSplitS);

    /**
     * get all instacne for the source
     * @return
     */
    protected abstract List<SourceInstance> fetchSourceInstances();

    /**
     * lock the source ,the lock is globel,only one source instance can get it in same time
     * @return
     */
    protected abstract boolean holdLockSourceInstance();

    /**
     *  unlock
     */
    protected abstract void unlockSourceInstance();



    /**
     * juge need add split，根据调度策略选择
     * 每次最大增加的分片数，根据调度次数决定
     * @param allSplits
     * @param sourceInstances
     * @param workingSplits
     * @return
     */
    protected SplitChanged getAdditionSplits(List<ISplit> allSplits, List<SourceInstance> sourceInstances, List<ISplit> workingSplits, List<ISplit> ownerSplits) {
        SplitChanged splitChanged = getChangedSplitCount(allSplits,sourceInstances,workingSplits.size(),ownerSplits.size());
        if(splitChanged == null){
            return null;
        }
        if(splitChanged.isNewSplit == false){
            return null;
        }
        if(splitChanged.splitCount <= 0){
            return null;
        }
        List<ISplit> noWorkingSplits = getNoWorkingSplits(allSplits,workingSplits);
        List<ISplit> newSplits = new ArrayList<>();
        for(int i = 0; i < noWorkingSplits.size(); i++){
            boolean success = holdLockSplit(noWorkingSplits.get(i));
            if(success){
                newSplits.add(noWorkingSplits.get(i));
                if(newSplits.size() >= splitChanged.splitCount){
                    break;
                }
            }
        }
        splitChanged.setChangedSplits(newSplits);
        return splitChanged;

    }



    protected List<ISplit> getNoWorkingSplits(List<ISplit> allSplits, List<ISplit> workingSplits) {
        Set<String> workingSplitIds = new HashSet<>();
        for(ISplit split:workingSplits){
            workingSplitIds.add(split.getQueueId());
        }
        List<ISplit> splits = new ArrayList<>();
        for(ISplit split:allSplits){
            if(!workingSplitIds.contains(split.getQueueId())){
                splits.add(split);
            }
        }
        return splits;
    }

    /**
     * 获取需要删除的分片
     * @param allSplits
     * @param sourceInstances
     * @param workingSplits
     * @return
     */
    protected SplitChanged getRemoveSplits(List<ISplit> allSplits, List<SourceInstance> sourceInstances, List<ISplit> workingSplits, List<ISplit> ownerSplits) {
        SplitChanged splitChanged = getChangedSplitCount(allSplits, sourceInstances, workingSplits.size(), ownerSplits.size());
        if(splitChanged == null){
            return null;
        }
        if(splitChanged.isNewSplit == true){
            return null;
        }

        if(splitChanged.splitCount <= 0){
            return null;
        }
        //List<ISplit> ownerSplits=source.ownerSplits();
        List<ISplit> removeSplits = new ArrayList();
        for(int i = 0; i < splitChanged.splitCount; i++){
           removeSplits.add(ownerSplits.get(i));
        }
        splitChanged.setChangedSplits(removeSplits);
        return splitChanged;
    }

    /**
     * 获取需要变动的分片个数，新增或删除
     * 分配策略，只有有未分配的分片时才会分配新分片，为了减少分片切换，前面几次尽可能少分，后面越来越多
     * @return 需要本实例有变化的分配，新增或删除
     */
    protected SplitChanged getChangedSplitCount(List<ISplit> allSplits, List<SourceInstance> sourceInstances, int splitCountInWorking, int ownerSplitCount){
        //int ownerSplitCount=source.ownerSplits().size();
        int instanceCount = sourceInstances.size();
        if(instanceCount == 0){
            instanceCount = 1;
        }
        int allSplitCount = allSplits.size();
        int minSplitCount = allSplitCount/instanceCount;
        int maxSplitCount = minSplitCount+(allSplitCount%instanceCount == 0 ? 0 : 1);
        //已经是最大分片数了
        if(ownerSplitCount == maxSplitCount){
            return null;
        }
        if(ownerSplitCount > maxSplitCount){
            int changeSplitCount = ownerSplitCount-maxSplitCount;
            return new SplitChanged(changeSplitCount,false);
        }
        //分片已经全部在处理，当前分片也符合最小分片分配策略，不需要重新分配
        if(splitCountInWorking == allSplitCount&&ownerSplitCount >= minSplitCount){
            return null;
        }
        //如果还有未分配的分片，且当前实例还有分片的可行性，则分配分片
        if(splitCountInWorking < allSplitCount && ownerSplitCount < maxSplitCount){
            int changeSplitCount = Math.min(maxSplitCount - ownerSplitCount, getMaxSplitCountInOneBalance());

            return new SplitChanged(changeSplitCount,true);
        }
        return null;
    }

    @Override
    public int getBalanceCount() {
        return balanceCount;
    }

    /**
     * 每次负载均衡最大的分片个数,目的是前几次，少分配分配，可能有实例在启动中，以免频繁切换分片，到后面实例都启动了，斤可能多分配分片
     * @return
     */
    private int getMaxSplitCountInOneBalance() {
        int balanceCount = getBalanceCount();
        return (int)Math.pow(2, balanceCount - 1);
    }

}
