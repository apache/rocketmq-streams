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
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.IteratorWrap;
import org.apache.rocketmq.streams.window.storage.RocksdbIterator;
import org.apache.rocketmq.streams.window.storage.WindowType;

import java.util.ArrayList;
import java.util.Comparator;
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
    public int doFireWindowInstance(WindowInstance instance) {
        String windowInstanceId = instance.getWindowInstanceId();
        String queueId = instance.getSplitId();

        RocksdbIterator<WindowBaseValue> rocksdbIterator = storage.getWindowBaseValue(queueId, windowInstanceId, WindowType.NORMAL_WINDOW, null);

        ArrayList<WindowValue> windowValues = new ArrayList<>();
        while (rocksdbIterator.hasNext()) {
            IteratorWrap<WindowBaseValue> next = rocksdbIterator.next();
            WindowValue data = (WindowValue)next.getData();
            windowValues.add(data);
        }

        windowValues.sort(Comparator.comparingLong(WindowBaseValue::getPartitionNum));

        int fireCount = sendBatch(windowValues, queueId, 0);

        clearFire(instance);

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
        Long startTime=System.currentTimeMillis();
        DebugWriter.getDebugWriter(getConfigureName()).writeShuffleCalcultateReceveMessage(instance, messages, queueId);

        List<String> sortKeys = new ArrayList<>();
        Map<String, List<IMessage>> groupBy = groupByGroupName(messages, sortKeys);

        RocksdbIterator<WindowBaseValue> windowBaseValue = storage.getWindowBaseValue(queueId, instance.getWindowInstanceId(), WindowType.NORMAL_WINDOW, null);

        ArrayList<WindowBaseValue> windowValues = new ArrayList<>();
        while (windowBaseValue.hasNext()) {
            IteratorWrap<WindowBaseValue> next = windowBaseValue.next();
            windowValues.add(next.getData());
        }

        Map<String, List<WindowValue>> temp = windowValues.stream().map((value) -> (WindowValue) value).collect(Collectors.groupingBy(WindowValue::getMsgKey));

        Map<String, List<WindowValue>> groupByMsgKey = new HashMap<>(temp);

        List<WindowValue> allWindowValues = new ArrayList<>();

        //处理不同groupBy的message
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

        saveStorage(instance.getWindowInstanceId(), queueId, allWindowValues);
    }


    protected void saveStorage(String windowInstanceId, String queueId, List<WindowValue> allWindowValues) {
        List<WindowBaseValue> temp = new ArrayList<>(allWindowValues);

        storage.putWindowBaseValue(queueId, windowInstanceId, WindowType.NORMAL_WINDOW, null, temp);
    }



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
    public boolean supportBatchMsgFinish() {
        return true;
    }

    protected void calculateWindowValue(WindowValue windowValue, IMessage msg) {
        windowValue.calculate(this, msg);

    }


    protected WindowValue createWindowValue(String queueId, String groupBy, WindowInstance instance) {
        WindowValue windowValue = new WindowValue();
        windowValue.setStartTime(instance.getStartTime());
        windowValue.setEndTime(instance.getEndTime());
        windowValue.setFireTime(instance.getFireTime());
        windowValue.setGroupBy(groupBy == null ? "" : groupBy);
        windowValue.setMsgKey(MapKeyUtil.createKey(queueId, instance.getWindowInstanceId(), groupBy));
        String shuffleId = shuffleChannel.getChannelQueue(groupBy).getQueueId();
        windowValue.setPartitionNum(createPartitionNum(queueId, instance));
        windowValue.setPartition(shuffleId);
        windowValue.setWindowInstanceId(instance.getWindowInstanceId());

        return windowValue;
    }

    protected long createPartitionNum(String shuffleId, WindowInstance instance) {
        return incrementAndGetSplitNumber(instance, shuffleId);
    }


    protected static String createStoreKey(String shuffleId, String groupByKey, WindowInstance windowInstance) {
        return MapKeyUtil.createKey(shuffleId, windowInstance.getWindowInstanceId(), groupByKey);
    }


    @Override
    public void clearFireWindowInstance(WindowInstance windowInstance) {
        boolean canClear = windowInstance.isCanClearResource();

        if (canClear) {
            logoutWindowInstance(windowInstance.createWindowInstanceTriggerId());

            //清理MaxPartitionNum
            storage.deleteMaxPartitionNum(windowInstance.getSplitId(), windowInstance.getWindowInstanceId());

            //清理WindowInstance
            storage.deleteWindowInstance(windowInstance.getSplitId(), this.getNameSpace(), this.getConfigureName(), windowInstance.getWindowInstanceId());

            //清理WindowValue
            storage.deleteWindowBaseValue(windowInstance.getSplitId(), windowInstance.getWindowInstanceId(), WindowType.NORMAL_WINDOW, null);
        }

    }

    @Override
    public void clearCache(String queueId) {
        storage.clearCache(queueId);
    }


}
