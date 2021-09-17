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
package org.apache.rocketmq.streams.window.offset;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.sqlcache.SQLCache;

public class WindowMaxValueManager implements IWindowMaxValueManager {
    protected AbstractWindow window;
    protected Map<String,WindowMaxValueProcessor> windowMaxValueProcessorMap=new HashMap<>();
    protected transient ExecutorService executorService;
    protected transient SQLCache sqlCache;
    public WindowMaxValueManager(AbstractWindow window, SQLCache sqlCache){
        this.window=window;
        this.sqlCache=sqlCache;
        this.executorService=new ThreadPoolExecutor(10, 10,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
    }


    protected WindowMaxValueProcessor getOrCreate(String queueId){
        WindowMaxValueProcessor windowMaxValueProcessor=windowMaxValueProcessorMap.get(queueId);
        if(windowMaxValueProcessor==null){
            synchronized (this){
                windowMaxValueProcessor=windowMaxValueProcessorMap.get(queueId);
                if(windowMaxValueProcessor==null){
                    windowMaxValueProcessor=new WindowMaxValueProcessor(queueId,this.window,sqlCache);
                    windowMaxValueProcessorMap.put(queueId,windowMaxValueProcessor);
                }
            }
        }
        return windowMaxValueProcessor;
    }

    @Override
    public Long incrementAndGetSplitNumber(WindowInstance instance, String splitId) {
       return getOrCreate(splitId).incrementAndGetSplitNumber(instance);
    }

    @Override public WindowMaxValue querySplitNum(WindowInstance instance, String splitId) {
       return getOrCreate(splitId).querySplitNum(instance);
    }

    @Override public void initMaxSplitNum(WindowInstance windowInstance, Long maxSplitNum) {
        getOrCreate(windowInstance.getSplitId()).initMaxSplitNum(windowInstance,maxSplitNum);
    }

    //    @Override
//    public void flush(String... queueIds){
//       if(queueIds==null||queueIds.length==0){
//           return;
//       }
//       if(queueIds.length==1){
//           getOrCreate(queueIds[0]).flush();
//           return;
//       }
//        CountDownLatch countDownLatch = new CountDownLatch(queueIds.length);
//        for(String splitId:queueIds){
//            executorService.execute(new Runnable() {
//                @Override public void run() {
//                    getOrCreate(splitId).flush();
//                    countDownLatch.countDown();
//                }
//            });
//
//        }
//        try {
//            countDownLatch.await();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    @Override
    public void resetSplitNum(WindowInstance instance, String splitId) {
       getOrCreate(splitId).resetSplitNum(instance);
    }

    @Override
    public void deleteSplitNum(WindowInstance instance, String splitId) {
        getOrCreate(splitId).deleteSplitNum(instance);
    }


    @Override public Map<String, WindowMaxValue> saveMaxOffset(boolean isLong, String name,String shuffleId,Map<String, String> queueId2Offsets) {
        return getOrCreate(shuffleId).saveMaxOffset(isLong,name,queueId2Offsets);
    }

    @Override public Map<String, String> loadOffsets(String name,String shuffleId) {
        return getOrCreate(shuffleId).loadOffset(name);
    }

    @Override public Map<String, WindowMaxValue> queryOffsets(String name, String shuffleId,Set<String> oriQueueIds) {
        return getOrCreate(shuffleId).queryOffsets(name,oriQueueIds);
    }

    @Override
    public synchronized void removeKeyPrefixFromLocalCache(Set<String> queueIds) {
        for(String queueId:queueIds){
            getOrCreate(queueId).removeKeyPrefixFromLocalCache();
        }

    }


}
