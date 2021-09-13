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
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.SQLUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.db.driver.DriverBuilder;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

/**
 * an implementation of session window to save extra memory for different group by window instances
 * TODO：
 * 1）时间的处理上是否使用绝对值更好一些，毫秒的比较可能过于严格
 * 2）当缓存中存在数据，先shuffleCalculate再fireWindowInstance，后者似乎没必要了
 * 3）Debug类似TraceUtil，考虑后续去掉TraceUtil
 *
 * @author arthur
 */
public class SessionWindow extends WindowOperator {

    protected static final Log LOG = LogFactory.getLog(SessionWindow.class);

    public static final String SESSION_WINDOW_BEGIN_TIME = "1970-01-01";

    public static final String SESSION_WINDOW_END_TIME = "9999-01-01";

    private static final String SESSION_DATETIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    private static final String ORDER_BY_FIRE_TIME_PREFIX = "_order_by_fire_time_";

    /**
     * 会话窗口的超时时间，时间单位时秒
     */
    protected int sessionTimeOut = 10 * 60;

    public int getSessionTimeOut() {
        return sessionTimeOut;
    }

    public void setSessionTimeOut(int sessionTimeOut) {
        this.sessionTimeOut = sessionTimeOut;
    }

    @Override
    protected boolean initConfigurable() {
        //
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
        String windowInstanceId = instance.createWindowInstanceId();
        if (!windowInstanceMap.containsKey(windowInstanceId)) {
            Pair<Date, Date> startEndPair = getSessionTime(message);
            Date fireDate = DateUtil.addDate(TimeUnit.SECONDS, startEndPair.getRight(), waterMarkMinute * timeUnitAdjust);
            instance.setFireTime(DateUtil.format(fireDate, SESSION_DATETIME_PATTERN));
            windowInstanceMap.put(windowInstanceId, instance);
        }
        return new ArrayList<WindowInstance>() {{
            add(windowInstanceMap.get(windowInstanceId));
        }};
    }

    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        /**
         * 1、消息分组：获得分组的groupBy值和对应的消息
         * 2、获取已有所有分组的窗口计算结果：1）通过queueId、instance和groupBy计算存储的key；2）调用存储的获取接口；
         * 3、消息聚合计算：对每一条消息，如果存在可以合并的窗口，则修改窗口信息（备注：窗口信息的维护都是在WindowValue而非WindowInstance；窗口信息的改变对应的存储也要进行清除）；如果不存在，则创建新的会话窗口
         * 4、窗口的触发判断：对计算完的窗口进行触发时间判断，如果可以触发，则进行窗口的触发，并进行窗口的清除，同时进行fire time的更新；
         * 5、窗口结果存储：剩余的未触发的窗口结果进行本地存储和远程存储，并进行窗口的排序存储；
         */
        //
        List<String> groupSortedByOffset = new ArrayList<>();
        Map<String, List<IMessage>> groupBy = groupByGroupName(messages, groupSortedByOffset);
        int groupSize = groupSortedByOffset.size();
        //
        List<String> storeKeyList = new ArrayList<>(groupSize);
        Map<String, String> value2StoreMap = new HashMap<>(groupSize);
        for (String groupValue : groupSortedByOffset) {
            String storeKey = createStoreKey(queueId, groupValue, instance);
            storeKeyList.add(storeKey);
            value2StoreMap.put(groupValue, storeKey);
        }
        Map<String, List<WindowValue>> storeValueMap = storage.multiGetList(WindowValue.class, storeKeyList);
        //
        Long currentFireTime = DateUtil.parse(instance.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
        Long nextFireTime = currentFireTime + 1000 * 60 * 30;
        Iterator<Map.Entry<String, List<IMessage>>> iterator = groupBy.entrySet().iterator();
        Map<String, List<WindowValue>> resultMap = new HashMap<>(groupSize);
        List<WindowValue> toFireValueList = new ArrayList<>();
        while (iterator.hasNext()) {
            Map.Entry<String, List<IMessage>> entry = iterator.next();
            String groupValue = entry.getKey();
            String storeKey = value2StoreMap.get(groupValue);
            List<IMessage> groupMessageList = entry.getValue();
            Map<Long, WindowValue> id2ValueMap = new HashMap<>(groupMessageList.size());
            List<WindowValue> valueList = storeValueMap.getOrDefault(storeKey, new ArrayList<>());
            for (IMessage message : groupMessageList) {
                //
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
                //
                Long itFireTime = DateUtil.parse(windowValue.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
                if (itFireTime > currentFireTime && itFireTime < nextFireTime) {
                    nextFireTime = itFireTime;
                }
            }
            //
            Iterator<Map.Entry<Long, WindowValue>> it = id2ValueMap.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, WindowValue> theEntry = it.next();
                Long id = theEntry.getKey();
                WindowValue value = theEntry.getValue();
                if (checkFire(queueId, value)) {
                    try {
                        String result = new String(Base64Utils.decode(value.getComputedColumnResult()), "UTF-8");
                        TraceUtil.debug(String.valueOf(id), "shuffle message fire", value.getStartTime(), value.getEndTime(), result);
                    } catch (Exception e) {

                    }
                    toFireValueList.add(value);
                    it.remove();
                }
                try {
                    String result = new String(Base64Utils.decode(value.getComputedColumnResult()), "UTF-8");
                    TraceUtil.debug(String.valueOf(id), "shuffle message store", value.getStartTime(), value.getEndTime(), result);
                } catch (Exception e) {

                }

            }
            if (!id2ValueMap.isEmpty()) {
                resultMap.put(storeKey, new ArrayList<>(id2ValueMap.values()));
            }
        }
        //
        doFire(queueId, instance, toFireValueList, currentFireTime, nextFireTime);
        //
        store(resultMap, instance, queueId);
    }

    private boolean checkFire(String queueId, WindowValue value) {
        Long maxEventTime = getMaxEventTime(queueId);
        //set current time if not every queue have arrived
        //TODO 连同WindowFireSource里的触发条件，需要讨论
        if (maxEventTime == null) {
            maxEventTime = System.currentTimeMillis();
        }
        Long fireTime = DateUtil.parse(value.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
        if (fireTime < maxEventTime) {
            return true;
        }
        return false;
    }

    private void doFire(String queueId, WindowInstance instance, List<WindowValue> valueList, Long currentFireTime,
        Long nextFireTime) {
        if (CollectionUtil.isEmpty(valueList)) {
            return;
        }
        valueList.sort(Comparator.comparingLong(WindowBaseValue::getPartitionNum));
        sendFireMessage(valueList, queueId);
        clearWindowValues(valueList, queueId, instance);
        //
        if (nextFireTime.equals(currentFireTime)) {
            String instanceId = instance.createWindowInstanceId();
            if (windowInstanceMap.containsKey(instanceId)) {
                windowInstanceMap.get(instanceId).setFireTime(DateUtil.format(new Date(nextFireTime)));
                windowFireSource.registFireWindowInstanceIfNotExist(instance, this);
            } else {
                LOG.error("window instance lost, queueId: " + queueId + " ,fire time" + instance.getFireTime());
            }
        }
    }

    private WindowValue queryOrCreateWindowValue(WindowInstance windowInstance, String queueId, String groupByValue,
        IMessage message, List<WindowValue> valueList, String storeKey) {
        //
        if (CollectionUtil.isEmpty(valueList)) {
            return createWindowValue(queueId, groupByValue, windowInstance, message, storeKey);
        }
        //
        for (WindowValue value : valueList) {
            Date sessionBegin = DateUtil.parseTime(value.getStartTime());
            Date sessionEnd = DateUtil.parseTime(value.getEndTime());
            Pair<Date, Date> startEndPair = getSessionTime(message);
            Date messageBegin = startEndPair.getLeft();
            Date messageEnd = startEndPair.getRight();
            if (messageBegin.compareTo(sessionBegin) >= 0 && messageBegin.compareTo(sessionEnd) < 0) {
                sessionEnd = messageEnd;
                Date sessionFire = DateUtil.addDate(TimeUnit.SECONDS, sessionEnd, waterMarkMinute * timeUnitAdjust);
                value.setEndTime(DateUtil.format(sessionEnd, SESSION_DATETIME_PATTERN));
                //clean order storage as sort field 'fireTime' changed
                String existPartitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(value), groupByValue), windowInstance);
                storage.getLocalStorage().removeKeys(new ArrayList<String>() {{
                    add(existPartitionNumKey);
                }});
                //
                value.setFireTime(DateUtil.format(sessionFire, SESSION_DATETIME_PATTERN));
                return value;
            } else if (messageBegin.compareTo(sessionBegin) < 0 && messageEnd.compareTo(sessionBegin) > 0) {
                sessionBegin = messageBegin;
                value.setStartTime(DateUtil.format(sessionBegin, SESSION_DATETIME_PATTERN));
                return value;
            }
        }
        return createWindowValue(queueId, groupByValue, windowInstance, message, storeKey);
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

    protected void store(Map<String, List<WindowValue>> group2ValueMap, WindowInstance windowInstance,
        String queueId) {
        //
        if (CollectionUtil.isEmpty(group2ValueMap)) {
            return;
        }
        //
        storage.multiPutList(group2ValueMap, windowInstance.createWindowInstanceId(), queueId, sqlCache);
        //
        Map<String, WindowValue> allValueMap = new HashMap<>();
        Iterator<Map.Entry<String, List<WindowValue>>> iterator = group2ValueMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, List<WindowValue>> entry = iterator.next();
            String groupByValue = entry.getKey();
            List<WindowValue> valueList = entry.getValue();
            for (WindowValue value : valueList) {
                //keep same prefix with fireWindowInstance()
                String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(value), groupByValue), windowInstance);
                allValueMap.put(partitionNumKey, value);
            }
        }
        storage.getLocalStorage().multiPut(allValueMap);
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
        value.setNameSpace(getNameSpace());
        value.setConfigureName(getConfigureName());
        Pair<Date, Date> startEndPair = getSessionTime(message);
        String startTime = DateUtil.format(startEndPair.getLeft(), SESSION_DATETIME_PATTERN);
        String endTime = DateUtil.format(startEndPair.getRight(), SESSION_DATETIME_PATTERN);
        String fireTime = DateUtil.format(DateUtil.addDate(TimeUnit.SECONDS, startEndPair.getRight(), waterMarkMinute * timeUnitAdjust), SESSION_DATETIME_PATTERN);
        value.setStartTime(startTime);
        value.setEndTime(endTime);
        value.setFireTime(fireTime);
        value.setGroupBy(groupBy);
        value.setMsgKey(StringUtil.createMD5Str(storeKey));
        //FIXME shuffleId vs queueId TODO delete assert
        String shuffleId = shuffleChannel.getChannelQueue(groupBy).getQueueId();
        assert shuffleId.equalsIgnoreCase(queueId);
        value.setPartitionNum(createPartitionNum(value, queueId, instance));
        value.setPartition(shuffleId);
        value.setWindowInstancePartitionId(instance.getWindowInstanceKey());
        value.setWindowInstanceId(instance.getWindowInstanceKey());
        return value;
    }

    protected static String getOrderBypFieldName(WindowValue windowValue) {
        //TODO 测试是否按照触发时间由小到大排序
        return MapKeyUtil.createKey(windowValue.getFireTime(), windowValue.getPartitionNum() + "");
    }

    protected static String getOrderBypPrefix() {
        return ORDER_BY_FIRE_TIME_PREFIX;
    }

    /**
     * update session's next fire time
     *
     * @param windowInstance
     * @param queueId
     * @param queueId2Offset
     * @return
     */
    @Override
    public int fireWindowInstance(WindowInstance windowInstance, String queueId, Map<String, String> queueId2Offset) {
        //get iterator sorted by fire time
        WindowBaseValueIterator<WindowValue> it = storage.loadWindowInstanceSplitData(getOrderBypPrefix(), queueId, windowInstance.createWindowInstanceId(), null, getWindowBaseValueClass());
        //
        if (queueId2Offset != null) {
            String offset = queueId2Offset.get(queueId);
            if (StringUtil.isNotEmpty(offset)) {
                it.setPartitionNum(Long.valueOf(offset));
            }
        }
        //
        Long currentFireTime = DateUtil.parse(windowInstance.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
        Long nextFireTime = currentFireTime + 1000 * 60 * 30;
        List<WindowValue> toFireValueList = new ArrayList<>();
        while (it.hasNext()) {
            WindowValue windowValue = it.next();
            if (windowValue == null) {
                continue;
            }
            if (checkFire(queueId, windowValue)) {
                TraceUtil.debug(String.valueOf(id), "shuffle message fire", windowValue.getStartTime(), windowValue.getEndTime(), windowValue.getComputedColumnResult());
                toFireValueList.add(windowValue);
            } else {
                Long itFireTime = DateUtil.parse(windowValue.getFireTime(), SESSION_DATETIME_PATTERN).getTime();
                if (itFireTime > currentFireTime && itFireTime < nextFireTime) {
                    nextFireTime = itFireTime;
                    break;
                }
            }
        }
        doFire(queueId, windowInstance, toFireValueList, currentFireTime, nextFireTime);
        //
        return toFireValueList.size();
    }

    /**
     * clear has fired window value
     *
     * @param windowValues
     * @param queueId
     * @param instance
     */
    protected void clearWindowValues(List<WindowValue> windowValues, String queueId, WindowInstance instance) {
        if (windowValues == null || windowValues.size() == 0) {
            return;
        }
        Set<String> deleteKeys = new HashSet<>();
        List<String> msgKeys = new ArrayList<>();
        for (WindowValue windowValue : windowValues) {
            String storeKey = createStoreKey(queueId, windowValue.getGroupBy(), instance);
            String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()) + "", instance);
            deleteKeys.add(storeKey);
            deleteKeys.add(partitionNumKey);
            msgKeys.add(windowValue.getMsgKey());
        }
        String sql = "delete from window_value where msg_key in(" + SQLUtil.createInSql(msgKeys) + ")";
        DriverBuilder.createDriver().execute(sql);
        storage.getLocalStorage().removeKeys(deleteKeys);
    }

    @Override
    public long incrementAndGetSplitNumber(WindowInstance instance, String shuffleId) {
        long numer = super.incrementAndGetSplitNumber(instance, shuffleId);
        if (numer > 900000000) {
            this.getWindowMaxValueManager().resetSplitNum(instance, shuffleId);
        }
        return numer;
    }
}
