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

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
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
import org.apache.rocketmq.streams.window.storage.ShufflePartitionManager;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowOperator extends AbstractShuffleWindow {
    private static final String ORDER_BY_SPLIT_NUM = "_order_by_split_num_";

    protected transient AtomicInteger fireCountAccumulator = new AtomicInteger(0);

    protected transient int windowValueCount = 0;

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

    @Override
    public int fireWindowInstance(WindowInstance instance, String queueId, Map<String, String> queueId2Offset) {
        List<WindowValue> windowValues = new ArrayList<>();
        int fireCount = 0;

        WindowBaseValueIterator<WindowBaseValue> it = storage.loadWindowInstanceSplitData(getOrderBypPrefix(), queueId, instance.createWindowInstanceId(), null, getWindowBaseValueClass());
        if (queueId2Offset != null) {
            String offset = queueId2Offset.get(queueId);
            if (StringUtil.isNotEmpty(offset)) {
                it.setPartitionNum(Long.valueOf(offset));
            }
        }

        while (it.hasNext()) {
            WindowBaseValue windowBaseValue = it.next();
            if (windowBaseValue == null) {
                continue;
            }
            WindowValue windowValue = (WindowValue) windowBaseValue;

            Integer currentValue = getValue(windowValue, "total");

            fireCountAccumulator.addAndGet(currentValue);
            windowValues.add((WindowValue) windowBaseValue);
            if (windowValues.size() >= windowCache.getBatchSize()) {
                sendFireMessage(windowValues, queueId);

                fireCount += windowValues.size();
                windowValues = new ArrayList<>();
            }

        }
        if (windowValues.size() > 0) {
            sendFireMessage(windowValues, queueId);
            fireCount += windowValues.size();
        }
        clearFire(instance);
        this.sqlCache.addCache(new FiredNotifySQLElement(queueId, instance.createWindowInstanceId()));
        return fireCount;
    }


    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        DebugWriter.getDebugWriter(getConfigureName()).writeShuffleCalcultateReceveMessage(instance, messages, queueId);
        List<String> sortKeys = new ArrayList<>();
        Map<String, List<IMessage>> groupBy = groupByGroupName(messages, sortKeys);
        Set<String> groupByKeys = groupBy.keySet();
        List<String> storeKeys = new ArrayList<>();
        for (String groupByKey : groupByKeys) {
            String storeKey = createStoreKey(queueId, groupByKey, instance);
            storeKeys.add(storeKey);
        }
        Map<String, WindowBaseValue> allWindowValues = new HashMap<>();
        //从存储中，查找window value对象，value是对象的json格式
        Map<String, WindowBaseValue> existWindowValues = storage.multiGet(getWindowBaseValueClass(), storeKeys, instance.createWindowInstanceId(), queueId);

        for (String groupByKey : sortKeys) {

            List<IMessage> msgs = groupBy.get(groupByKey);
            String storeKey = createStoreKey(queueId, groupByKey, instance);
            WindowValue windowValue = (WindowValue) existWindowValues.get(storeKey);

            if (windowValue == null) {
                windowValueCount++;
                windowValue = createWindowValue(queueId, groupByKey, instance);
            }
            allWindowValues.put(storeKey, windowValue);
            windowValue.incrementUpdateVersion();

            if (msgs != null) {
                for (IMessage message : msgs) {
                    calculateWindowValue(windowValue, message);
                }
            }
        }

        if (DebugWriter.getDebugWriter(this.getConfigureName()).isOpenDebug()) {
            DebugWriter.getDebugWriter(this.getConfigureName()).writeWindowCalculate(this, new ArrayList(allWindowValues.values()), queueId);
        }

        saveStorage(allWindowValues, instance, queueId);
    }

    private Integer getValue(WindowValue windowValue, String fieldName) {
        Object value = windowValue.getComputedColumnResultByKey(fieldName);
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        } else if (value instanceof String) {
            String strValue = (String) value;
            return Integer.valueOf(strValue);
        }
        throw new ClassCastException("value:[" + value + "] of fieldName:[" + fieldName + "] can not change to number.");
    }

    protected void saveStorage(Map<String, WindowBaseValue> allWindowValues, WindowInstance windowInstance, String queueId) {
        String windowInstanceId = windowInstance.createWindowInstanceId();

        storage.multiPut(allWindowValues, windowInstanceId, queueId, sqlCache);
        Map<String, WindowBaseValue> partionNumOrders = new HashMap<>();//需要基于key前缀排序partitionnum
        for (WindowBaseValue windowBaseValue : allWindowValues.values()) {
            WindowValue windowValue = (WindowValue) windowBaseValue;
            String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);
            partionNumOrders.put(partitionNumKey, windowValue);
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
    protected Map<String, List<IMessage>> groupByGroupName(List<IMessage> messages, List<String> sortKeys) {
        if (messages == null || messages.size() == 0) {
            return new HashMap<>();
        }
        Map<String, List<IMessage>> groupBy = new HashMap<>();
        Map<String, MessageOffset> minOffsets = new HashMap<>();
        for (IMessage message : messages) {
            String groupByValue = generateShuffleKey(message);
            if (StringUtil.isEmpty(groupByValue)) {
                groupByValue = "<null>";
            }
            List<IMessage> messageList = groupBy.computeIfAbsent(groupByValue, k -> new ArrayList<>());
            MessageOffset minOffset = minOffsets.get(groupByValue);
            if (minOffset == null) {
                minOffset = message.getHeader().getMessageOffset();
            } else {
                if (minOffset.greateThan(message.getHeader().getOffset())) {
                    minOffset = message.getHeader().getMessageOffset();
                }
            }
            minOffsets.put(groupByValue, minOffset);
            messageList.add(message);
        }


        List<Entry<String, MessageOffset>> sortByMinOffset = new ArrayList<>(minOffsets.entrySet());
        sortByMinOffset.sort((o1, o2) -> {
            if (o1.getValue().equals(o2.getValue())) {
                return 0;
            }
            boolean success = o1.getValue().greateThan(o2.getValue().getOffsetStr());
            if (success) {
                return 1;
            } else {
                return -1;
            }
        });
        for (Entry<String, MessageOffset> entry : sortByMinOffset) {
            sortKeys.add(entry.getKey());
        }
        return groupBy;
    }

    @Override
    protected Long queryWindowInstanceMaxSplitNum(WindowInstance instance) {
        return storage.getMaxSplitNum(instance, getWindowBaseValueClass());
    }

    @Override
    public boolean supportBatchMsgFinish() {
        return true;
    }

    protected void calculateWindowValue(WindowValue windowValue, IMessage msg) {
        windowValue.calculate(this, msg);

    }

    /**
     * 创建新的window value对象
     *
     * @param groupBy
     * @param instance
     * @return
     */
    protected WindowValue createWindowValue(String queueId, String groupBy, WindowInstance instance) {
        WindowValue windowValue = new WindowValue();
        windowValue.setStartTime(instance.getStartTime());
        windowValue.setEndTime(instance.getEndTime());
        windowValue.setFireTime(instance.getFireTime());
        windowValue.setGroupBy(groupBy == null ? "" : groupBy);
        windowValue.setMsgKey(StringUtil.createMD5Str(MapKeyUtil.createKey(queueId, instance.createWindowInstanceId(), groupBy)));
        String shuffleId = shuffleChannel.getChannelQueue(groupBy).getQueueId();
        windowValue.setPartitionNum(createPartitionNum(windowValue, queueId, instance));
        windowValue.setPartition(shuffleId);
        windowValue.setWindowInstancePartitionId(instance.getWindowInstanceKey());
        windowValue.setWindowInstanceId(instance.getWindowInstanceKey());

        return windowValue;

    }

    protected long createPartitionNum(WindowValue windowValue, String shuffleId, WindowInstance instance) {
        return incrementAndGetSplitNumber(instance, shuffleId);
    }

    /**
     * 创建存储key
     *
     * @param groupByKey
     * @param windowInstance
     * @return
     */
    protected static String createStoreKey(String shuffleId, String groupByKey, WindowInstance windowInstance) {
        return MapKeyUtil.createKey(shuffleId, windowInstance.createWindowInstanceId(), groupByKey);
    }

    /**
     * 需要排序的前缀
     *
     * @return
     */
    protected static String getOrderBypPrefix() {
        return ORDER_BY_SPLIT_NUM;
    }

    /**
     * 需要排序的字段值
     *
     * @return
     */
    protected static String getOrderBypFieldName(WindowValue windowValue) {
        return windowValue.getPartitionNum() + "";
    }

    /**
     * 删除掉触发过的数据
     *
     * @param windowInstance
     */
    @Override
    public void clearFireWindowInstance(WindowInstance windowInstance) {
        String partitionNum = (getOrderBypPrefix() + windowInstance.getSplitId());

        boolean canClear = windowInstance.isCanClearResource();

        if (canClear) {
            logoutWindowInstance(windowInstance.createWindowInstanceTriggerId());
            windowMaxValueManager.deleteSplitNum(windowInstance, windowInstance.getSplitId());
            ShufflePartitionManager.getInstance().clearWindowInstance(windowInstance.createWindowInstanceId());
            storage.delete(windowInstance.createWindowInstanceId(), windowInstance.getSplitId(), getWindowBaseValueClass(), sqlCache);
            storage.getLocalStorage().delete(windowInstance.createWindowInstanceId(), partitionNum, getWindowBaseValueClass());
            if (!isLocalStorageOnly) {
                WindowInstance.clearInstance(windowInstance, sqlCache);
            }
        }

    }

    @Override
    public void clearCache(String queueId) {
        getStorage().clearCache(shuffleChannel.getChannelQueue(queueId), getWindowBaseValueClass());
        getStorage().clearCache(getOrderByQueue(queueId, getOrderBypPrefix()), getWindowBaseValueClass());
        ShufflePartitionManager.getInstance().clearSplit(queueId);
    }

    public ISplit getOrderByQueue(String key, String prefix) {
        int index = shuffleChannel.hash(key);
        ISplit targetQueue = shuffleChannel.getQueueList().get(index);
        return new ISplit() {
            @Override
            public String getQueueId() {
                return prefix + targetQueue.getQueueId();
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


    public static class WindowRowOperator implements IRowOperator {

        protected WindowInstance windowInstance;
        protected String spiltId;
        protected AbstractWindow window;

        public WindowRowOperator(WindowInstance windowInstance, String spiltId, AbstractWindow window) {
            this.windowInstance = windowInstance;
            this.spiltId = spiltId;
            this.window = window;
        }

        @Override
        public synchronized void doProcess(Map<String, Object> row) {
            WindowValue windowValue = ORMUtil.convert(row, WindowValue.class);
            List<String> keys = new ArrayList<>();
            String storeKey = createStoreKey(spiltId, windowValue.getGroupBy(), windowInstance);
            keys.add(storeKey);
            String storeOrderKey = createStoreKey(getOrderBypPrefix() + windowValue.getPartition(), MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);
            Map<String, WindowBaseValue> valueMap = window.getStorage().getLocalStorage().multiGet(WindowValue.class, keys);
            if (CollectionUtil.isEmpty(valueMap)) {
                Map<String, WindowBaseValue> map = new HashMap<>(4);

                map.put(storeKey, windowValue);
                map.put(storeOrderKey, windowValue);
                window.getStorage().getLocalStorage().multiPut(map);
                return;
            }
            WindowValue localValue = (WindowValue) valueMap.values().iterator().next();
            if (windowValue.getUpdateVersion() > localValue.getUpdateVersion()) {
                Map<String, WindowBaseValue> map = new HashMap<>();
                map.put(storeKey, windowValue);
                map.put(storeOrderKey, windowValue);
                window.getStorage().getLocalStorage().multiPut(map);
            }
        }
    }

}
