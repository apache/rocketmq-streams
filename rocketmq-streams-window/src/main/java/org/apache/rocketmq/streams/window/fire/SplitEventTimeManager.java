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
package org.apache.rocketmq.streams.window.fire;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class SplitEventTimeManager {
    protected static final Log LOG = LogFactory.getLog(SplitEventTimeManager.class);
    protected static Map<String,Long> messageSplitId2MaxTime=new HashMap<>();
    private AtomicInteger queueIdCount=new AtomicInteger(0);
    protected Long lastUpdateTime;

    protected volatile Integer allSplitSize;
    protected volatile Integer workingSplitSize;
    protected Map<String, List<ISplit>> splitsGroupByInstance;
    protected ISource source;

    protected volatile boolean isAllSplitReceived=false;
    protected transient String queueId;

    private static Long splitReadyTime;

    public SplitEventTimeManager(ISource source,String queueId){
        this.source=source;
        this.queueId=queueId;
        if(source instanceof AbstractSource){
            AbstractSource abstractSource=(AbstractSource)source;
            List<ISplit> splits=abstractSource.getAllSplits();
            if(splits==null){
                this.allSplitSize=-1;
            }else {
                this.allSplitSize=splits.size();
            }
        }
    }

    public void updateEventTime(IMessage message, AbstractWindow window){
        String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
        if(StringUtil.isEmpty(oriQueueId)){
            return;
        }
        Long occurTime = WindowInstance.getOccurTime(window,message);
        if(occurTime==null){
            return;
        }
        Long oldTime=messageSplitId2MaxTime.get(oriQueueId);
        if(oldTime==null){
            queueIdCount.incrementAndGet();
            messageSplitId2MaxTime.put(oriQueueId,occurTime);
        }else {
            if(occurTime>oldTime){
                messageSplitId2MaxTime.put(oriQueueId,occurTime);
            }
        }
    }


    public Long getMaxEventTime(){

        if(!isSplitsReceiver()){
            return null;
        }
        Long min=null;
        Iterator<Map.Entry<String, Long>> it = messageSplitId2MaxTime.entrySet().iterator();
        while (it.hasNext()){
            Map.Entry<String, Long> entry=it.next();
            Long eventTime=entry.getValue();
            if(eventTime==null){
                return null;
            }
            if(min==null){
                min=eventTime;
            }else {
                if(eventTime<min){
                    min=eventTime;
                }
            }
        }
        return min;

    }


    protected boolean isSplitsReceiver(){
        if(isAllSplitReceived){
            return true;
        }
        if(lastUpdateTime==null){
            lastUpdateTime=System.currentTimeMillis();
        }
        if(allSplitSize==null&&workingSplitSize==null){
            if(source==null){
                return false;
            }
            if(source instanceof AbstractSource){
                AbstractSource abstractSource=(AbstractSource)source;
                List<ISplit> splits=abstractSource.getAllSplits();
                if(splits==null){
                    this.allSplitSize=-1;
                }else {
                    this.allSplitSize=splits.size();
                }
            }
        }
        if(allSplitSize==-1){
            return true;
        }
        if(allSplitSize!=-1&&workingSplitSize==null){
            workingSplitSize=0;
        }
        if(allSplitSize!=-1&&allSplitSize>workingSplitSize){
            if(System.currentTimeMillis()-lastUpdateTime>1000){
                workingSplitSize=calcuteWorkingSplitSize();
                lastUpdateTime=System.currentTimeMillis();
                if(allSplitSize>workingSplitSize){
                    return false;
                }
            }else {
                return false;
            }
        }
        if(this.splitsGroupByInstance==null){
            return false;
        }
        //add time out policy: no necessary waiting for other split
        if (splitReadyTime == null) {
            synchronized (this) {
                if (splitReadyTime == null) {
                    splitReadyTime = System.currentTimeMillis();
                }
            }
        }
        if (workingSplitSize == messageSplitId2MaxTime.size()) {
            this.isAllSplitReceived = true;
            return true;
        } else {
            if (System.currentTimeMillis() - splitReadyTime >= 1000 * 60) {
                this.isAllSplitReceived = true;
                return true;
            }
        }
        return false;
    }

    private Integer calcuteWorkingSplitSize() {
        if(source instanceof AbstractSource){
            AbstractSource abstractSource=(AbstractSource)source;
            Map<String, List<ISplit>> splits = abstractSource.getWorkingSplitsGroupByInstances();
            if(splits==null){
                return 0;
            }
            this.splitsGroupByInstance=splits;
            int count=0;
            for(List<ISplit> splitList:splits.values()){
                count+=splitList.size();
            }
            return count;
        }
        return 0;

    }

    public void setSource(ISource source) {
        this.source = source;
    }
}
