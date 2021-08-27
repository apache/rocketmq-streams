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
package org.apache.rocketmq.streams.window.operator.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.sqlcache.impl.FiredNotifySQLElement;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.IWindowStorage;
import org.apache.rocketmq.streams.window.storage.ShufflePartitionManager;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

public class WindowOperator extends AbstractShuffleWindow {

    private static final String ORDER_BY_SPLIT_NUM="_order_by_split_num_";//key=_order;queueid,windowinstanceid,partitionNum

    public WindowOperator() {
        super();
    }

    @Deprecated
    public WindowOperator(String timeFieldName, int windowPeriodMinute) {
        super();
        super.timeFieldName = timeFieldName;
        super.sizeInterval = windowPeriodMinute;
    }

    @Deprecated
    public WindowOperator(String timeFieldName, int windowPeriodMinute, String calFieldName) {
        super();
        super.timeFieldName = timeFieldName;
        super.sizeInterval = windowPeriodMinute;
    }

    public WindowOperator(int sizeInterval, String groupByFieldName, Map<String, String> select) {
        this.sizeInterval = sizeInterval;
        this.slideInterval = sizeInterval;
        this.groupByFieldName = groupByFieldName;
        this.setSelectMap(select);
    }

    protected transient AtomicInteger shuffleCount=new AtomicInteger(0);
    protected transient AtomicInteger fireCountAccumulator=new AtomicInteger(0);
    @Override
    public int fireWindowInstance(WindowInstance instance, String queueId,Map<String,String> queueId2Offset) {
        List<WindowValue> windowValues=new ArrayList<>();
        int fireCount=0;
        long startTime=System.currentTimeMillis();
        int sendCost=0;
        int currentCount=0;
        //for(String queueId:currentQueueIds){
        WindowBaseValueIterator<WindowBaseValue> it = storage.loadWindowInstanceSplitData(getOrderBypPrefix(),queueId,instance.createWindowInstanceId(),null,getWindowBaseValueClass());
        if(queueId2Offset!=null){
            String offset=queueId2Offset.get(queueId);
            if(StringUtil.isNotEmpty(offset)){
                it.setPartitionNum(Long.valueOf(offset));
            }
        }
        while (it.hasNext()){
            WindowBaseValue windowBaseValue=it.next();
            if(windowBaseValue==null){
                continue;
            }
            WindowValue windowValue=(WindowValue)windowBaseValue;
            Integer currentValue=(Integer)windowValue.getComputedColumnResultByKey("total");
            if(currentValue==null){
                currentValue=0;
            }
              fireCountAccumulator.addAndGet(currentValue);
            windowValues.add((WindowValue)windowBaseValue);
            if(windowValues.size()>=windowCache.getBatchSize()){
                long sendFireCost=System.currentTimeMillis();
                sendFireMessage(windowValues,queueId);
                sendCost+=(System.currentTimeMillis()-sendFireCost);
                fireCount+=windowValues.size();
                windowValues=new ArrayList<>();
            }

        }
        if(windowValues.size()>0){
            long sendFireCost=System.currentTimeMillis();
            sendFireMessage(windowValues,queueId);
            sendCost+=(System.currentTimeMillis()-sendFireCost);
            fireCount+=windowValues.size();
        }
//        if(fireCountAccumulator.get()>25000){
//            System.out.println("fire count is "+fireCountAccumulator.get());
//        }

        //long clearStart=System.currentTimeMillis();
        clearFire(instance);
        this.sqlCache.addCache(new FiredNotifySQLElement(queueId,instance.createWindowInstanceId()));
        // System.out.println("=============== fire cost is "+(System.currentTimeMillis()-startTime)+"send cost is "+sendCost+" clear cost is "+(System.currentTimeMillis()-clearStart));
        return fireCount;
    }

    protected transient Map<String,Integer>  shuffleWindowInstanceId2MsgCount=new HashMap<>();

    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        Map<String,List<IMessage>> groupBy=groupByGroupName(messages);
        Set<String> groupByKeys=groupBy.keySet();
        List<String> storeKeys=new ArrayList<>();
        for(String groupByKey:groupByKeys){
            String storeKey=createStoreKey(queueId,groupByKey,instance);
            storeKeys.add(storeKey);
        }
        Map<String,WindowBaseValue> exisitWindowValues=new HashMap<>();
        Map<String,WindowBaseValue> newWindowValues=new HashMap<>();
        List<WindowValue> windowValues=new ArrayList<>();
        //从存储中，查找window value对象，value是对象的json格式
        Map<String, WindowBaseValue>  key2WindowValues=storage.multiGet(getWindowBaseValueClass(),storeKeys,instance.createWindowInstanceId(),queueId);
      //  Iterator<Entry<String, List<IMessage>>> it = groupBy.entrySet().iterator();
        List<String> groupbys=new ArrayList<>(groupBy.keySet());
        Collections.sort(groupbys);
        for(String groupByKey:groupbys){

            List<IMessage> msgs=groupBy.get(groupByKey);
            String storeKey=createStoreKey(queueId,groupByKey,instance);
            WindowValue windowValue=(WindowValue)key2WindowValues.get(storeKey);;
            if(windowValue==null){
                windowValue=createWindowValue(queueId,groupByKey,instance);
                windowValue.setOrigOffset(msgs.get(0).getHeader().getOffset());
                newWindowValues.put(storeKey,windowValue);
            }else {
                exisitWindowValues.put(storeKey,windowValue);
            }

            windowValues.add(windowValue);
            windowValue.incrementUpdateVersion();
            Integer origValue=(Integer)windowValue.getComputedColumnResultByKey("total");
            if(origValue==null){
                origValue=0;
            }
            if(msgs!=null){
                for(IMessage message:msgs){
                    windowValue.calculate(this,message);
                }
            }

            Integer currentValue=(Integer)windowValue.getComputedColumnResultByKey("total");
            if(currentValue==null){
                currentValue=0;
            }
            shuffleCount.addAndGet(-origValue);
            shuffleCount.addAndGet(currentValue);
//            if(shuffleCount.get()>25000){
//                System.out.println("==========shuffle count is "+shuffleCount.get());
//            }


        }
        if(DebugWriter.getDebugWriter(this.getConfigureName()).isOpenDebug()){
            DebugWriter.getDebugWriter(this.getConfigureName()).writeWindowCalculate(this,windowValues,queueId);
        }
        Map<String, WindowBaseValue> allWindowValues=new HashMap<>();
        allWindowValues.putAll(newWindowValues);
        allWindowValues.putAll(exisitWindowValues);
        saveStorage(allWindowValues,instance,queueId);
        //Integer count=shuffleWindowInstanceId2MsgCount.get(instance.createWindowInstanceId());
        //if(count==null){
        //    count=0;
        //}
        //count+=messages.size();
        //shuffleWindowInstanceId2MsgCount.put(instance.createWindowInstanceId(),count);
    }


    protected void saveStorage(Map<String, WindowBaseValue> allWindowValues,WindowInstance windowInstance,String queueId) {
        String windowInstanceId=windowInstance.createWindowInstanceId();

        storage.multiPut(allWindowValues,windowInstanceId,queueId,sqlCache);
        Map<String,WindowBaseValue> partionNumOrders=new HashMap<>();//需要基于key前缀排序partitionnum
        for(WindowBaseValue windowBaseValue:allWindowValues.values()){
            WindowValue windowValue=(WindowValue)windowBaseValue;
            String partitionNumKey=createStoreKey(getOrderBypPrefix()+queueId,MapKeyUtil.createKey(getOrderBypFieldName(windowValue),windowValue.getGroupBy()),windowInstance);
            partionNumOrders.put(partitionNumKey,windowValue);
        }
        storage.getLocalStorage().multiPut(partionNumOrders);
    }

    @Override
    public Class getWindowBaseValueClass() {
        return WindowValue.class;
    }

    /**
     * 按group name 进行分组
     *
     * @param messages
     * @return
     */
    protected Map<String, List<IMessage>> groupByGroupName(List<IMessage> messages) {
        if(messages==null||messages.size()==0){
            return new HashMap<>();
        }
        Map<String,List<IMessage>> groupBy=new HashMap<>();
        for(IMessage message:messages){
            String groupByValue=generateShuffleKey(message);
            if(StringUtil.isEmpty(groupByValue)){
                groupByValue="<null>";
            }
            List<IMessage> messageList=groupBy.get(groupByValue);
            if(messageList==null){
                messageList=new ArrayList<>();
                groupBy.put(groupByValue,messageList);
            }
            messageList.add(message);
        }
        return groupBy;
    }

    @Override protected Long queryWindowInstanceMaxSplitNum(WindowInstance instance) {
        return storage.getMaxSplitNum(instance,getWindowBaseValueClass());
    }

    /**
     * 创建新的window value对象
     *
     * @param groupBy
     * @param instance
     * @return
     */
    protected WindowValue createWindowValue(String queueId,String groupBy, WindowInstance instance) {
        WindowValue windowValue = new WindowValue();
        windowValue.setNameSpace(getNameSpace());
        windowValue.setConfigureName(getConfigureName());
        windowValue.setStartTime(instance.getStartTime());
        windowValue.setEndTime(instance.getEndTime());
        windowValue.setFireTime(instance.getFireTime());
        windowValue.setGroupBy(groupBy==null?"":groupBy);
        windowValue.setMsgKey(StringUtil.createMD5Str(MapKeyUtil.createKey(queueId, instance.createWindowInstanceId(), groupBy)));
        String shuffleId=shuffleChannel.getChannelQueue(groupBy).getQueueId();
        windowValue.setPartitionNum(createPartitionNum(windowValue,queueId,instance));
        windowValue.setPartition(shuffleId);
        windowValue.setWindowInstancePartitionId(instance.getWindowInstanceKey());
        windowValue.setWindowInstanceId(instance.getWindowInstanceKey());

        return windowValue;

    }

    protected long createPartitionNum(WindowValue windowValue, String shuffleId,  WindowInstance instance) {
        return incrementAndGetSplitNumber(instance,shuffleId);
    }

    /**
     * 创建存储key
     *
     * @param groupByKey
     * @param windowInstance
     * @return
     */
    protected static String createStoreKey(String shuffleId, String groupByKey,WindowInstance windowInstance){
        String storeKey = MapKeyUtil.createKey(shuffleId, windowInstance.createWindowInstanceId(), groupByKey);
        return storeKey;
    }

    /**
     * 需要排序的前缀
     *
     * @return
     */
    protected static String getOrderBypPrefix(){
        return ORDER_BY_SPLIT_NUM;
    }

    /**
     * 需要排序的字段值
     *
     * @return
     */
    protected  static String getOrderBypFieldName(WindowValue windowValue){
        return windowValue.getPartitionNum()+"";
    }

    /**
     * 删除掉触发过的数据
     *
     * @param windowInstance
     */
    @Override
    public void clearFireWindowInstance(WindowInstance windowInstance) {
        String partitionNum=(getOrderBypPrefix()+ windowInstance.getSplitId());

        boolean canClear=false;
        if(fireMode!=2){
            canClear=true;
        }
        if(fireMode==2){
            Date endTime=DateUtil.parse(windowInstance.getEndTime());
            Date lastDate = DateUtil.addSecond(endTime, waterMarkMinute * timeUnitAdjust);

            if((windowInstance.getLastMaxUpdateTime()>lastDate.getTime())){
                canClear=true;
            }
        }

        if(canClear){
            this.windowInstanceMap.remove(windowInstance.createWindowInstanceId());

            windowMaxValueManager.deleteSplitNum(windowInstance,windowInstance.getSplitId());
            ShufflePartitionManager.getInstance().clearWindowInstance(windowInstance.createWindowInstanceId());
            storage.delete(windowInstance.createWindowInstanceId(),windowInstance.getSplitId(),getWindowBaseValueClass(),sqlCache);
            storage.getLocalStorage().delete(windowInstance.createWindowInstanceId(),partitionNum,getWindowBaseValueClass());
            if(!isLocalStorageOnly){
                WindowInstance.clearInstance(windowInstance,sqlCache);
            }
        }


    }

    @Override
    public void clearCache(String queueId) {
        getStorage().clearCache(shuffleChannel.getChannelQueue(queueId),getWindowBaseValueClass());
        getStorage().clearCache(getOrderByQueue(queueId,getOrderBypPrefix()),getWindowBaseValueClass());
        ShufflePartitionManager.getInstance().clearSplit(queueId);
    }

    public ISplit getOrderByQueue(String key,String prefix){
        int index=shuffleChannel.hash(key);
        ISplit targetQueue = shuffleChannel.getQueueList().get(index);
        return new ISplit() {
            @Override
            public String getQueueId() {
                return prefix+targetQueue.getQueueId();
            }

            @Override
            public String getPlusQueueId() {
                return prefix+targetQueue.getPlusQueueId();
            }

            @Override
            public Object getQueue() {
                return targetQueue.getQueue();
            }

            @Override
            public int compareTo(Object o) {
                return targetQueue.compareTo(o);
            }

            @Override
            public String toJson() {
                return targetQueue.toJson();
            }

            @Override
            public void toObject(String jsonString) {
                targetQueue.toObject(jsonString);
            }
        };
    }

    public static void compareAndSet(WindowInstance windowInstance, IWindowStorage storage,List<WindowValue> windowValues){
        if(windowValues==null||storage==null){
            return;
        }
        synchronized (storage){
            List<String> storeKeys = new ArrayList<>();
            Map<String,WindowValue> windowValueMap=new HashMap<>();
            for(WindowValue windowValue:windowValues){
                String storeKey=createStoreKey(windowValue.getPartition(),windowValue.getGroupBy(),windowInstance);
                storeKeys.add(storeKey);
                windowValueMap.put(storeKey,windowValue);
                String storeOrderKey=createStoreKey(windowValue.getPartition(),windowValue.getPartitionNum()+"",windowInstance);
                windowValueMap.put(storeOrderKey,windowValue);
            }
            Map<String, WindowBaseValue> valueMap = storage.multiGet(WindowValue.class,storeKeys);
            if(valueMap==null||valueMap.size()==0){
                storage.multiPut(windowValueMap);
                return;
            }
            Iterator<Entry<String, WindowBaseValue>> it = valueMap.entrySet().iterator();

            while (it.hasNext()){
                Entry<String, WindowBaseValue> entry=it.next();
                String storeKey=entry.getKey();
                WindowBaseValue localValue=entry.getValue();
                WindowValue windowValue=windowValueMap.get(storeKey);
                if (windowValue.getUpdateVersion() <= localValue.getUpdateVersion()) {
                    windowValueMap.remove(storeKey);
                }
            }
            if(CollectionUtil.isNotEmpty(windowValueMap)){
                storage.multiPut(windowValueMap);
            }
        }
    }

    public static class WindowRowOperator implements IRowOperator {

        protected WindowInstance windowInstance;
        protected String spiltId;
        protected AbstractWindow window;

        public WindowRowOperator(WindowInstance windowInstance,String spiltId,AbstractWindow window){
            this.windowInstance=windowInstance;
            this.spiltId=spiltId;
            this.window=window;
        }

        @Override
        public synchronized void doProcess(Map<String, Object> row) {
            WindowValue windowValue = ORMUtil.convert(row, WindowValue.class);
            List<String> keys = new ArrayList<>();
            String storeKey=createStoreKey(spiltId,windowValue.getGroupBy(),windowInstance);
            keys.add(storeKey);
            String storeOrderKey=createStoreKey(getOrderBypPrefix()+windowValue.getPartition(),MapKeyUtil.createKey(getOrderBypFieldName(windowValue),windowValue.getGroupBy()),windowInstance);
            Map<String, WindowBaseValue> valueMap = window.getStorage().getLocalStorage().multiGet(WindowValue.class,keys);
            if (CollectionUtil.isEmpty(valueMap)) {
                Map<String, WindowBaseValue> map = new HashMap<>(4);

                map.put(storeKey, windowValue);
                map.put(storeOrderKey, windowValue);
                window.getStorage().getLocalStorage().multiPut(map);
                return;
            }
            WindowValue localValue = (WindowValue)valueMap.values().iterator().next();
            if (windowValue.getUpdateVersion() > localValue.getUpdateVersion()) {
                Map<String, WindowBaseValue> map = new HashMap<>();
                map.put(storeKey, windowValue);
                map.put(storeOrderKey, windowValue);
                window.getStorage().getLocalStorage().multiPut(map);
            }
        }
    }

}
