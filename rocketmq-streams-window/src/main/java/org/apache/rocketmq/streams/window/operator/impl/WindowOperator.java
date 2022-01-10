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

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.sqlcache.impl.FiredNotifySQLElement;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.ShufflePartitionManager;
import org.apache.rocketmq.streams.window.storage.rocketmq.WindowType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class WindowOperator extends AbstractShuffleWindow {
    public WindowOperator() {
        super();
    }

    @Override
    public int fireWindowInstance(WindowInstance instance, String queueId, Map<String, String> queueId2Offset) {
        String windowInstanceId = instance.createWindowInstanceId();

        List<WindowBaseValue> temp = storage.getWindowBaseValue(windowInstanceId, WindowType.NORMAL_WINDOW, null);

        if (temp == null || temp.size() == 0) {
            return 0;
        }

        ArrayList<WindowValue> windowValues = new ArrayList<>();
        temp.forEach(windowBaseValue -> windowValues.add((WindowValue) windowBaseValue));
        //todo 按照partition_num从小到达排序WindowValue

        int fireCount = sendBatch(windowValues, queueId, 0);

        clearFire(instance);

        //todo 去掉
        this.sqlCache.addCache(new FiredNotifySQLElement(queueId, windowInstanceId));

        return fireCount;
    }


    private int sendBatch(List<WindowValue> windowValues, String queueId, int fireCount) {
        if (windowValues == null || windowValues.size() == 0) {
            return fireCount;
        }

        if (windowValues.size() <= windowCache.getBatchSize()) {
            sendFireMessage(windowValues, queueId);

            fireCount += windowValues.size();

            return fireCount;
        } else {
            ArrayList<WindowValue> temp = new ArrayList<>();
            for (int i = 0; i < windowCache.getBatchSize(); i++) {
                temp.add(windowValues.remove(i));
            }

            sendFireMessage(temp, queueId);

            return sendBatch(windowValues, queueId, fireCount + windowCache.getBatchSize());
        }
    }


    @Override
    public void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId) {
        DebugWriter.getDebugWriter(getConfigureName()).writeShuffleCalcultateReceveMessage(instance, messages, queueId);

        List<String> sortKeys = new ArrayList<>();
        Map<String, List<IMessage>> groupBy = groupByGroupName(messages, sortKeys);

        List<WindowBaseValue> windowValues = storage.getWindowBaseValue(instance.createWindowInstanceId(), WindowType.NORMAL_WINDOW, null);

        Map<String, List<WindowValue>> groupByMsgKey = windowValues.stream()
                .map((value)-> (WindowValue)value)
                .collect(Collectors.groupingBy(WindowValue::getMsgKey));

        List<WindowValue> allWindowValues = new ArrayList<>();

        for (String groupByKey : sortKeys) {
            List<IMessage> msgs = groupBy.get(groupByKey);
            String storeKey = createStoreKey(queueId, groupByKey, instance);

            //msgKey 为唯一键
            List<WindowValue> windowValueList = groupByMsgKey.get(storeKey);
            WindowValue windowValue;
            if (windowValueList == null || windowValueList.size() == 0) {
                windowValue = createWindowValue(queueId, groupByKey, instance);
            } else {
                windowValue = windowValueList.get(0);
            }

            allWindowValues.add(windowValue);
            windowValue.incrementUpdateVersion();

            if (msgs != null) {
                for (IMessage message : msgs) {
                    calculateWindowValue(windowValue, message);
                }
            }
        }

        if (DebugWriter.getDebugWriter(this.getConfigureName()).isOpenDebug()) {
            DebugWriter.getDebugWriter(this.getConfigureName()).writeWindowCalculate(this, allWindowValues, queueId);
        }

        saveStorage(instance.createWindowInstanceId(), allWindowValues);
    }


    protected void saveStorage(String windowInstanceId, List<WindowValue> allWindowValues) {
        long maxPartitionNum = Long.MIN_VALUE;
        long minPartitionNum = Long.MAX_VALUE;

        ArrayList<WindowBaseValue> temp = new ArrayList<>();

        for (WindowValue windowValue : allWindowValues) {
            temp.add(windowValue);

            //找到WindowValue中最大和最小的partition_num
            long partitionNum = windowValue.getPartitionNum();
            if (partitionNum > maxPartitionNum) {
                maxPartitionNum = partitionNum;
            }

            if (partitionNum < minPartitionNum) {
                minPartitionNum = partitionNum;
            }
        }

        storage.putWindowBaseValue(windowInstanceId, WindowType.NORMAL_WINDOW, null, temp);

        Long remoteMaxPartitionNum = storage.getMaxPartitionNum(windowInstanceId, WindowType.NORMAL_WINDOW, null);
        if (remoteMaxPartitionNum == null || maxPartitionNum > remoteMaxPartitionNum) {
            storage.putMaxPartitionNum(windowInstanceId, WindowType.NORMAL_WINDOW, null, maxPartitionNum);
        }

        Long remoteMinPartitionNum = storage.getMaxPartitionNum(windowInstanceId, WindowType.NORMAL_WINDOW, null);
        if (remoteMinPartitionNum == null || minPartitionNum < remoteMinPartitionNum) {
            storage.putMinPartitionNum(windowInstanceId, WindowType.NORMAL_WINDOW, null, minPartitionNum);
        }


//        storage.multiPut(allWindowValues, windowInstanceId, queueId, sqlCache);
//        Map<String, WindowBaseValue> partionNumOrders = new HashMap<>();//需要基于key前缀排序partitionnum
//        for (WindowBaseValue windowBaseValue : allWindowValues.values()) {
//            WindowValue windowValue = (WindowValue) windowBaseValue;
//            String partitionNumKey = createStoreKey(getOrderBypPrefix() + queueId, MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);
//            partionNumOrders.put(partitionNumKey, windowValue);
//        }
//        storage.getLocalStorage().multiPut(partionNumOrders);
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
        return storage.getMaxPartitionNum(instance.createWindowInstanceId(), WindowType.NORMAL_WINDOW, null);
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
     * 删除掉触发过的数据
     *
     * @param windowInstance
     */
    @Override
    public void clearFireWindowInstance(WindowInstance windowInstance) {
        boolean canClear = windowInstance.isCanClearResource();

        if (canClear) {
            logoutWindowInstance(windowInstance.createWindowInstanceTriggerId());
            //todo,按照msgKey删除window_max_value中记录
            windowMaxValueManager.deleteSplitNum(windowInstance, windowInstance.getSplitId());
            //清除 内存中windowInstanceId已经准备完毕的标识。
            ShufflePartitionManager.getInstance().clearWindowInstance(windowInstance.createWindowInstanceId());

            storage.deleteWindowBaseValue(windowInstance.createWindowInstanceId(), WindowType.NORMAL_WINDOW, null);

            //todo WindowInstance没有保存到db中
            if (!isLocalStorageOnly) {
                WindowInstance.clearInstance(windowInstance, sqlCache);
            }
        }

    }

    @Override
    public void clearCache(String queueId) {
        //todo 从内存中删除数据
//        getStorage().clearCache(shuffleChannel.getChannelQueue(queueId), getWindowBaseValueClass());
//        getStorage().clearCache(getOrderByQueue(queueId, getOrderBypPrefix()), getWindowBaseValueClass());
        ShufflePartitionManager.getInstance().clearSplit(queueId);
    }


    /**
     * todo 将远程load到本地
     */
//    public static class WindowRowOperator implements IRowOperator {
//
//        protected WindowInstance windowInstance;
//        protected String spiltId;
//        protected AbstractWindow window;
//
//        public WindowRowOperator(WindowInstance windowInstance, String spiltId, AbstractWindow window) {
//            this.windowInstance = windowInstance;
//            this.spiltId = spiltId;
//            this.window = window;
//        }
//
//        @Override
//        public synchronized void doProcess(Map<String, Object> row) {
//            WindowValue windowValue = ORMUtil.convert(row, WindowValue.class);
//            List<String> keys = new ArrayList<>();
//            String storeKey = createStoreKey(spiltId, windowValue.getGroupBy(), windowInstance);
//            keys.add(storeKey);
//            String storeOrderKey = createStoreKey(getOrderBypPrefix() + windowValue.getPartition(), MapKeyUtil.createKey(getOrderBypFieldName(windowValue), windowValue.getGroupBy()), windowInstance);
//            Map<String, WindowBaseValue> valueMap = window.getStorage().getLocalStorage().multiGet(WindowValue.class, keys);
//            if (CollectionUtil.isEmpty(valueMap)) {
//                Map<String, WindowBaseValue> map = new HashMap<>(4);
//
//                map.put(storeKey, windowValue);
//                map.put(storeOrderKey, windowValue);
//                window.getStorage().getLocalStorage().multiPut(map);
//                return;
//            }
//            WindowValue localValue = (WindowValue) valueMap.values().iterator().next();
//            if (windowValue.getUpdateVersion() > localValue.getUpdateVersion()) {
//                Map<String, WindowBaseValue> map = new HashMap<>();
//                map.put(storeKey, windowValue);
//                map.put(storeOrderKey, windowValue);
//                window.getStorage().getLocalStorage().multiPut(map);
//            }
//        }
//    }

}
