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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.IteratorWrap;
import org.apache.rocketmq.streams.window.storage.RocksdbIterator;
import org.apache.rocketmq.streams.window.storage.WindowType;

/**
 * an implementation of session window to save extra memory for different group by window instances
 *
 * @author arthur
 */
public class SessionOperator extends WindowOperator {

    protected static final Log LOG = LogFactory.getLog(SessionOperator.class);

    public static final String SESSION_WINDOW_BEGIN_TIME = "1970-01-01";

    public static final String SESSION_WINDOW_END_TIME = "9999-01-01";

    private static final String SESSION_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * 会话窗口的超时时间，时间单位时秒，默认10分钟
     */
    protected int sessionTimeOut = 10 * 60;

    private transient Object lock = new Object();

    public SessionOperator() {

    }

    public SessionOperator(Integer timeout) {
        this.sessionTimeOut = Optional.ofNullable(timeout).orElse(sessionTimeOut);
    }


    @Override
    protected boolean initConfigurable() {
        return super.initConfigurable();
    }

    /**
     * one queue own only one window instance, init the first fire time for the instance
     *
     * @param message
     * @param queueId
     * @return
     */
    @Override
    public List<WindowInstance> queryOrCreateWindowInstance(IMessage message, String queueId) {
        WindowInstance instance = createWindowInstance(SESSION_WINDOW_BEGIN_TIME, SESSION_WINDOW_END_TIME, null, queueId);
        String windowInstanceId = instance.getWindowInstanceId();
        WindowInstance existWindowInstance = searchWindowInstance(windowInstanceId);
        if (existWindowInstance == null) {
            Pair<Date, Date> startEndPair = getSessionTime(message);
            Date fireDate = DateUtil.addDate(TimeUnit.SECONDS, startEndPair.getRight(), waterMarkMinute * timeUnitAdjust);
            //out of order data, normal fire mode considered only
            Long maxEventTime = getMaxEventTime(queueId);
            if (maxEventTime == null) {
                LOG.warn("use current time as max event time!");
                maxEventTime = System.currentTimeMillis();
            }
            if (fireDate.getTime() <= maxEventTime) {
                LOG.warn("message is discarded as out of date! fire time: " + fireDate.getTime() + " max event time: " + maxEventTime);
                return new ArrayList<>();
            }
            instance.setFireTime(DateUtil.format(fireDate, SESSION_DATETIME_PATTERN));
            registerWindowInstance(instance);
        }
        return new ArrayList<WindowInstance>() {{
            add(searchWindowInstance(windowInstanceId));
        }};
    }

    @Override
    public WindowInstance registerWindowInstance(WindowInstance windowInstance) {
        return super.registerWindowInstance(windowInstance.getWindowInstanceId(), windowInstance);
    }

    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        /**
         * 1、消息分组：获得分组的groupBy值和对应的消息
         * 2、获取已有所有分组的窗口计算结果：1）通过queueId、instance和groupBy计算存储的key；2）调用存储的获取接口；
         * 3、消息聚合计算：对每一条消息，如果存在可以合并的窗口，则修改窗口信息（备注：窗口信息的维护都是在WindowValue而非WindowInstance；窗口信息的改变对应的存储也要进行清除）；如果不存在，则创建新的会话窗口
         * 4、窗口的合并计算：新消息导致不同窗口合并成一个大的会话窗口
         * 5、窗口结果存储：剩余的未触发的窗口结果进行本地存储（覆盖和新增）和远程存储（先删除，后增加），并进行窗口的前缀存储（覆盖和新增）；
         */
        synchronized (lock) {
            //
            List<String> groupSortedByOffset = new ArrayList<>();
            Map<String, List<IMessage>> groupBy = groupByGroupName(messages, groupSortedByOffset);
            int groupSize = groupSortedByOffset.size();
            //
            Map<String, String> value2StoreMap = new HashMap<>(groupSize);
            for (String groupValue : groupSortedByOffset) {
                String storeKey = createStoreKey(queueId, groupValue, instance);
                value2StoreMap.put(groupValue, storeKey);
            }

            List<WindowBaseValue> windowBaseValue = new ArrayList<>();

            RocksdbIterator<List<WindowBaseValue>> rocksdbIterator = storage.getWindowBaseValueList(instance.getSplitId(),
                    instance.getWindowInstanceId(), WindowType.SESSION_WINDOW, null);
            while (rocksdbIterator.hasNext()) {
                IteratorWrap<List<WindowBaseValue>> next = rocksdbIterator.next();
                windowBaseValue.addAll(next.getData());
            }

            //1、按照storeKey过滤
            //2、将WindowBaseValue转化成WindowValue
            List<String> storeKeys = new ArrayList<>();
            for (String groupValue : groupBy.keySet()) {
                String storeKey = createStoreKey(queueId, groupValue, instance);
                storeKeys.add(storeKey);
            }

            Map<String, List<WindowValue>> storeValueMap = windowBaseValue.stream()
                    .map((value) -> (WindowValue) value)
                    .filter((value) -> {
                        for (String storeKey : storeKeys) {
                            if (storeKey.equalsIgnoreCase(value.getMsgKey())) {
                                return true;
                            }
                        }
                        return false;
                    }).collect(Collectors.groupingBy(WindowValue::getMsgKey));


            Iterator<Map.Entry<String, List<IMessage>>> iterator = groupBy.entrySet().iterator();
            Map<String, List<WindowValue>> resultMap = new HashMap<>(groupSize);
            while (iterator.hasNext()) {
                Map.Entry<String, List<IMessage>> entry = iterator.next();
                String groupValue = entry.getKey();
                String storeKey = value2StoreMap.get(groupValue);
                List<IMessage> groupMessageList = entry.getValue();
                Map<Long, WindowValue> id2ValueMap = new HashMap<>(groupMessageList.size());
                List<WindowValue> valueList = storeValueMap.getOrDefault(storeKey, new ArrayList<>());
                for (WindowValue value : valueList) {
                    id2ValueMap.put(value.getPartitionNum(), value);
                }

                //对消息挨条处理找到windowValue
                for (IMessage message : groupMessageList) {
                    WindowValue windowValue = queryOrCreateWindowValue(instance, queueId, groupValue, message, valueList, storeKey);
                    windowValue.calculate(this, message);
                    //region trace
                    String traceId = message.getMessageBody().getString("SHUFFLE_TRACE_ID");
                    if (!StringUtil.isEmpty(traceId)) {
                        try {
                            String result = new String(Base64Utils.decode(windowValue.getComputedColumnResult()), "UTF-8");
                            TraceUtil.debug(traceId, "shuffle message out " + groupValue, String.valueOf(windowValue.getPartitionNum()), windowValue.getStartTime(), windowValue.getEndTime(), result);
                        } catch (Exception e) {

                        }
                    }
                    //endregion
                    id2ValueMap.put(windowValue.getPartitionNum(), windowValue);
                }
                //merge values,
                List<WindowValue> groupValueList = mergeWindowValue(new ArrayList<>(id2ValueMap.values()), instance);
                resultMap.put(storeKey, groupValueList);
            }
            //
            store(resultMap, instance, queueId);
        }

    }

    //创建windowValue，如果已经存储的valueList中有WindowValue窗口已经包含新来message，那么merge下
    private WindowValue queryOrCreateWindowValue(WindowInstance windowInstance, String queueId, String groupByValue,
                                                 IMessage message, List<WindowValue> valueList, String storeKey) {
        //
        if (CollectionUtil.isEmpty(valueList)) {
            return createWindowValue(queueId, groupByValue, windowInstance, message, storeKey);
        }
        //put keys to be deleted here and delete them at last
        List<String> deletePrefixKeyList = new ArrayList<>();
        //
        for (WindowValue value : valueList) {
            Date sessionBegin = DateUtil.parseTime(value.getStartTime());
            Date sessionEnd = DateUtil.parseTime(value.getEndTime());
            Pair<Date, Date> startEndPair = getSessionTime(message);
            Date messageBegin = startEndPair.getLeft();
            Date messageEnd = startEndPair.getRight();
            if (messageBegin.compareTo(sessionBegin) >= 0 && messageBegin.compareTo(sessionEnd) < 0) {
                //已经存储WindowValue窗口包含了新来message，合并窗口
                sessionEnd = messageEnd;
                Date sessionFire = DateUtil.addDate(TimeUnit.SECONDS, sessionEnd, waterMarkMinute * timeUnitAdjust);
                value.setEndTime(DateUtil.format(sessionEnd, SESSION_DATETIME_PATTERN));
                //clean order storage as sort field 'fireTime' changed
                //deleteMergeWindow(windowInstance.getWindowInstanceId(), value.getPartition(), value.getFireTime(), value.getPartitionNum(), value.getGroupBy());
                //
                value.setFireTime(DateUtil.format(sessionFire, SESSION_DATETIME_PATTERN));
                return value;
            } else if (messageBegin.compareTo(sessionBegin) < 0 && messageEnd.compareTo(sessionBegin) > 0) {
                sessionBegin = messageBegin;
                value.setStartTime(DateUtil.format(sessionBegin, SESSION_DATETIME_PATTERN));
                return value;
            }
        }
        //
        WindowValue newValue = createWindowValue(queueId, groupByValue, windowInstance, message, storeKey);
        valueList.add(newValue);
        return newValue;
    }

    private List<WindowValue> mergeWindowValue(List<WindowValue> allValueList, WindowInstance windowInstance) {
        if (allValueList.size() <= 1) {
            return allValueList;
        }

        //合并后需要删除的WindowValue
        Map<Integer/*被合并的索引*/, Integer/*合并key的索引*/> deleteValueMap = new HashMap<>(allValueList.size());

        //
        Map<Integer, List<Integer>> mergeValueMap = new HashMap<>(allValueList.size());
        Collections.sort(allValueList, Comparator.comparing(WindowValue::getStartTime));


        for (int outIndex = 0; outIndex < allValueList.size(); outIndex++) {
            if (deleteValueMap.containsKey(outIndex)) {
                continue;
            }
            int finalOutIndex = outIndex;
            mergeValueMap.put(outIndex, new ArrayList<Integer>() {{
                add(finalOutIndex);
            }});
            WindowValue outValue = allValueList.get(outIndex);
            for (int inIndex = outIndex + 1; inIndex < allValueList.size(); inIndex++) {
                WindowValue inValue = allValueList.get(inIndex);

                //新来message的点落在 上一个窗口内,合并这两个点，形成一个窗口
                if (0 <= inValue.getStartTime().compareTo(outValue.getStartTime()) && inValue.getStartTime().compareTo(outValue.getFireTime()) <= 0) {
                    deleteValueMap.put(inIndex, outIndex);
                    outValue.setEndTime(outValue.getEndTime().compareTo(inValue.getEndTime()) <= 0 ? inValue.getEndTime() : outValue.getEndTime());
                    outValue.setFireTime(outValue.getFireTime().compareTo(inValue.getFireTime()) <= 0 ? inValue.getFireTime() : outValue.getFireTime());
                    mergeValueMap.get(outIndex).add(inIndex);
                } else {
                    break;
                }
            }
        }
        Iterator<Map.Entry<Integer, List<Integer>>> iterator = mergeValueMap.entrySet().iterator();
        List<WindowValue> resultList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<Integer, List<Integer>> entry = iterator.next();
            WindowValue theValue = allValueList.get(entry.getKey());
            List<Integer> indexList = entry.getValue();
            WindowValue tempValue = WindowValue.mergeWindowValue(this, indexList.stream().map(index -> allValueList.get(index)).collect(Collectors.toList()));
            theValue.setComputedColumnResult(tempValue.getComputedColumnResult());
            theValue.setAggColumnResult(tempValue.getAggColumnResult());
            resultList.add(theValue);
        }

        deleteValueMap.keySet().forEach(key -> {
            WindowValue windowValue = allValueList.get(key);
            deleteMergeWindow(windowInstance.getWindowInstanceId(), windowValue.getPartition(),
                    windowValue.getFireTime(), windowValue.getPartitionNum(), windowValue.getGroupBy());
        });
        return resultList;
    }


    private void deleteMergeWindow(String windowInstanceId, String queueId, String fireTime, long partitionNum, String groupBy) {
        RocksdbIterator<List<WindowBaseValue>> windowBaseValueWrap = storage.getWindowBaseValueList(queueId, windowInstanceId, WindowType.SESSION_WINDOW, null);

        ArrayList<WindowValue> deleteList = new ArrayList<>();

        while (windowBaseValueWrap.hasNext()) {
            IteratorWrap<List<WindowBaseValue>> wrap = windowBaseValueWrap.next();
            List<WindowBaseValue> windowValue = wrap.getData();

            for (WindowBaseValue item : windowValue) {
                WindowValue temp = (WindowValue) item;
                if (temp.getPartition().equals(queueId) && temp.getPartitionNum() == partitionNum
                        && temp.getGroupBy().equals(groupBy) && temp.getFireTime().equals(fireTime)) {
                    deleteList.add(temp);
                }
            }
        }

        //删除
        for (WindowValue windowValue : deleteList) {
            storage.deleteWindowBaseValue(windowValue.getPartition(), windowValue.getWindowInstanceId(), WindowType.SESSION_WINDOW, null, windowValue.getMsgKey());
        }

//        windowBaseValueWrap = storage.getWindowBaseValue(queueId, windowInstanceId, WindowType.SESSION_WINDOW, null);
//
//        ArrayList<WindowValue> storeList = new ArrayList<>();
//        while (windowBaseValueWrap.hasNext()) {
//            IteratorWrap<WindowValue> next = windowBaseValueWrap.next();
//            WindowValue windowValue = next.getData();
//
//        }
//
//        if (windowBaseValueWrap.hasNext()) {
//            storage.putWindowBaseValueIterator(queueId, windowInstanceId, WindowType.SESSION_WINDOW, null, windowBaseValueWrap);
//        }
    }

    private Pair<Date, Date> getSessionTime(IMessage message) {
        Long occurTime = System.currentTimeMillis();
        try {
            occurTime = WindowInstance.getOccurTime(this, message);
        } catch (Exception e) {
            LOG.error("failed in computing occur time from the message!", e);
        }
        Date occurDate = new Date(occurTime);
        Date endDate = DateUtil.addDate(TimeUnit.SECONDS, occurDate, sessionTimeOut);
        return Pair.of(occurDate, endDate);
    }

    protected void store(Map<String, List<WindowValue>> key2ValueMap, WindowInstance windowInstance, String queueId) {
        if (CollectionUtil.isEmpty(key2ValueMap)) {
            return;
        }

        for (String storeKey : key2ValueMap.keySet()) {
            List<WindowBaseValue> temp = key2ValueMap.get(storeKey).stream()
                    .map(value -> (WindowBaseValue) value)
                    .collect(Collectors.toList());

            storage.putWindowBaseValue(queueId, windowInstance.getWindowInstanceId(), WindowType.SESSION_WINDOW, null, temp);
        }

    }

    /**
     * create new session window value
     *
     * @param queueId
     * @param groupBy
     * @param instance
     * @return
     */
    protected WindowValue createWindowValue(String queueId, String groupBy, WindowInstance instance, IMessage message,
                                            String storeKey) {
        WindowValue value = new WindowValue();
        Pair<Date, Date> startEndPair = getSessionTime(message);
        String startTime = DateUtil.format(startEndPair.getLeft(), SESSION_DATETIME_PATTERN);
        String endTime = DateUtil.format(startEndPair.getRight(), SESSION_DATETIME_PATTERN);
        String fireTime = DateUtil.format(DateUtil.addDate(TimeUnit.SECONDS, startEndPair.getRight(), waterMarkMinute * timeUnitAdjust), SESSION_DATETIME_PATTERN);
        value.setStartTime(startTime);
        value.setEndTime(endTime);
        value.setFireTime(fireTime);
        value.setGroupBy(groupBy);
        value.setMsgKey(storeKey);
        //FIXME shuffleId vs queueId TODO delete assert
        String shuffleId = shuffleChannel.getChannelQueue(groupBy).getQueueId();
        assert shuffleId.equalsIgnoreCase(queueId);
        value.setPartitionNum(createPartitionNum(queueId, instance));
        value.setPartition(shuffleId);
        value.setWindowInstanceId(instance.getWindowInstanceId());
        return value;
    }


    @Override
    public int doFireWindowInstance(WindowInstance windowInstance) {
        synchronized (lock) {
            String queueId = windowInstance.getSplitId();

            RocksdbIterator<List<WindowBaseValue>> windowBaseValue = storage.getWindowBaseValueList(queueId,
                    windowInstance.getWindowInstanceId(), WindowType.SESSION_WINDOW, null);

            ArrayList<WindowBaseValue> baseValues = new ArrayList<>();

            while (windowBaseValue.hasNext()) {
                IteratorWrap<List<WindowBaseValue>> next = windowBaseValue.next();
                List<WindowBaseValue> data = next.getData();
                baseValues.addAll(data);
            }

            List<WindowValue> result = baseValues.stream()
                    .map(value -> (WindowValue) value)
                    .sorted(Comparator.comparingLong(WindowBaseValue::getPartitionNum))
                    .collect(Collectors.toList());

//            Long currentFireTime = DateUtil.parse(windowInstance.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
//            Long nextFireTime = currentFireTime + 1000 * 60 * 1;
//            List<WindowValue> toFireValueList = new ArrayList<>();


//            for (WindowBaseValue baseValue : baseValues) {
//                WindowValue windowValue = (WindowValue) baseValue;
//                if (windowValue == null) {
//                    continue;
//                }
//
//                if (checkFire(queueId, windowValue)) {
//                    TraceUtil.debug(String.valueOf(windowValue.getPartitionNum()), "shuffle message fire", windowValue.getStartTime(), windowValue.getEndTime(), windowValue.getComputedColumnResult());
//                    toFireValueList.add(windowValue);
//                } else {
//                    Long itFireTime = DateUtil.parse(windowValue.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
//                    if (itFireTime > currentFireTime && itFireTime < nextFireTime) {
//                        nextFireTime = itFireTime;
//                        break;
//                    }
//                }
//            }
            doFire(queueId, windowInstance, result);
            return baseValues.size();
        }

    }

    private boolean checkFire(String queueId, WindowValue value) {
        Long maxEventTime = getMaxEventTime(queueId);
        //set current time if not every queue have arrived
        if (maxEventTime == null) {
            maxEventTime = System.currentTimeMillis();
        }
        Long fireTime = DateUtil.parse(value.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
        if (fireTime < maxEventTime) {
            System.out.printf("fire in sessionOperator: maxEventTime={%s}, fireTime={%s}", maxEventTime, fireTime);
            System.out.println("");
            return true;
        }
        return false;
    }


    private void doFire(String queueId, WindowInstance instance, List<WindowValue> valueList) {
        if (CollectionUtil.isEmpty(valueList)) {
            return;
        }

        valueList.sort(Comparator.comparingLong(WindowBaseValue::getPartitionNum));
        sendFireMessage(valueList, queueId);
        clearWindowValues(valueList, queueId, instance);
//
//        if (!nextFireTime.equals(currentFireTime)) {
//            String instanceId = instance.getWindowInstanceId();
//            WindowInstance existedWindowInstance = searchWindowInstance(instanceId);
//            if (existedWindowInstance != null) {
//                existedWindowInstance.setFireTime(DateUtil.format(new Date(nextFireTime)));
//                windowFireSource.registFireWindowInstanceIfNotExist(instance, this);
//            } else {
//                LOG.error("window instance lost, queueId: " + queueId + " ,fire time" + instance.getFireTime());
//            }
//        }
    }


    protected void clearWindowValues(List<WindowValue> deleteValueList, String queueId, WindowInstance instance) {
        if (CollectionUtil.isEmpty(deleteValueList)) {
            return;
        }

        Set<String> storeKeySet = new HashSet<>(deleteValueList.size());
        Set<Long> valueIdSet = new HashSet<>(deleteValueList.size());

        for (WindowValue windowValue : deleteValueList) {
            String storeKey = createStoreKey(queueId, windowValue.getGroupBy(), instance);
            Long valueId = windowValue.getPartitionNum();
            storeKeySet.add(storeKey);
            valueIdSet.add(valueId);
        }

        RocksdbIterator<List<WindowBaseValue>> rocksdbIterator = storage.getWindowBaseValueList(queueId, instance.getWindowInstanceId(), WindowType.SESSION_WINDOW, null);
        List<WindowBaseValue> temp = new ArrayList<>();
        while (rocksdbIterator.hasNext()) {
            IteratorWrap<List<WindowBaseValue>> next = rocksdbIterator.next();
            List<WindowBaseValue> data = next.getData();
            temp.addAll(data);
        }

        Map<String, List<WindowValue>> storeValueMap = temp.stream()
                .map((value) -> (WindowValue) value)
                .filter((value) -> {
                    for (String storeKey : storeKeySet) {
                        if (storeKey.equalsIgnoreCase(value.getMsgKey())) {
                            return true;
                        }
                    }
                    return false;
                }).collect(Collectors.groupingBy(WindowValue::getMsgKey));

        Map<String, List<WindowValue>> lastValueMap = new HashMap<>(storeValueMap.size());
        for (Map.Entry<String, List<WindowValue>> entry : storeValueMap.entrySet()) {
            String storeKey = entry.getKey();
            List<WindowValue> valueList = entry.getValue();
            valueList = valueList.stream().filter(value -> !valueIdSet.contains(value.getPartitionNum())).collect(Collectors.toList());
            lastValueMap.put(storeKey, valueList);
        }

        for (WindowValue windowValue : deleteValueList) {
            storage.deleteWindowBaseValue(queueId, instance.getWindowInstanceId(), WindowType.SESSION_WINDOW, null, windowValue.getMsgKey());
        }

        store(lastValueMap, instance, queueId);
    }


    @Override
    public long incrementAndGetSplitNumber(WindowInstance instance, String shuffleId) {
        long numer = super.incrementAndGetSplitNumber(instance, shuffleId);
        if (numer > 900000000) {
            this.storage.putMaxPartitionNum(shuffleId, instance.getWindowInstanceId(), numer);
        }
        return numer;
    }

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public void setSessionTimeOut(int sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
    }
}