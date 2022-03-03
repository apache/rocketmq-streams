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

package org.apache.rocketmq.streams.window.shuffle;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.window.debug.DebugWriter;
import org.apache.rocketmq.streams.window.model.WindowCache;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.operator.AbstractShuffleWindow;
import org.apache.rocketmq.streams.window.storage.IStorage;

/**
 * save receiver messages into cachefilter when checkpoint/autoflush/flush， process cachefilter message
 */
public class ShuffleCache extends WindowCache {
    protected AbstractShuffleWindow window;
    private HashMap<String, Boolean> hasLoad = new HashMap<>();

    public ShuffleCache(AbstractShuffleWindow window) {
        this.window = window;
    }

    /**
     * 调用时机：ShuffleChannel从上游读到shuffle数据，加入缓存后，
     * 满足条件： 定时/条数大于特定值/checkpoint/开始接收批量消息 时触发此方法
     *
     * @param messageList
     * @return
     */
    @Override
    protected boolean batchInsert(List<IMessage> messageList) {
        Map<Pair<String/*queueId*/, String/*windowInstanceId*/>, List<IMessage>> instance2Messages = new HashMap<>();
        Map<String/*windowInstanceId*/, WindowInstance> windowInstanceMap = new HashMap<>();

        groupByWindowInstanceAndQueueId(messageList, instance2Messages, windowInstanceMap);

        List<Pair<String, String>> keys = new ArrayList<>(instance2Messages.keySet());
        Collections.sort(keys);

        for (Pair<String, String> queueIdAndInstanceKey : keys) {
            String queueId = queueIdAndInstanceKey.getLeft();
            String windowInstanceId = queueIdAndInstanceKey.getRight();

            List<IMessage> messages = instance2Messages.get(queueIdAndInstanceKey);

            WindowInstance windowInstance = windowInstanceMap.get(windowInstanceId);

            DebugWriter.getDebugWriter(window.getConfigureName()).writeShuffleReceive(window, messages, windowInstance);

            stateMustLoad(queueId);

            window.shuffleCalculate(messages, windowInstance, queueId);

            //保存处理进度
            saveSplitProgress(queueId, messages);
        }
        return true;
    }

    private void stateMustLoad(String queueId) {
        Boolean load = this.hasLoad.get(queueId);
        if (load != null && load) {
            return;
        }

        //在计算之前需要异步加载状态完成
        HashMap<String, Future<?>> loadResult = this.window.getShuffleChannel().getLoadResult();
        Future<?> future = loadResult.get(queueId);

        if (future == null) {
            return;
        }

        try {
            long before = System.currentTimeMillis();
            future.get();
            long after = System.currentTimeMillis();

            System.out.println("message wait before state recover:[" + (after - before) + "] ms, queueId=" + queueId);

            hasLoad.put(queueId, true);
        } catch (Throwable t) {
            throw new RuntimeException("check remote with queueId:" + queueId + ",error", t);
        }
    }

    /**
     * save consumer progress（offset）for groupby  source shuffleId
     * window configName: name_window_10001
     * shuffleId: shuffle_NormalTestTopic_namespace_name_broker-a_001
     * oriQueueId: NormalTestTopic2_broker-a_000
     *
     * @param shuffleId
     * @param messages
     */
    protected void saveSplitProgress(String shuffleId, List<IMessage> messages) {
        IStorage delegator = this.window.getStorage();

        Map<String, String> queueId2OrigOffset = new HashMap<>();
        Boolean isLong = false;
        for (IMessage message : messages) {
            isLong = message.getMessageBody().getBoolean(ORIGIN_QUEUE_IS_LONG);
            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
            queueId2OrigOffset.put(oriQueueId, oriOffset);
        }

        for (String oriQueueId : queueId2OrigOffset.keySet()) {
            String currentOffset = queueId2OrigOffset.get(oriQueueId);

            String remoteMaxOffset = delegator.getMaxOffset(shuffleId, window.getConfigureName(), oriQueueId);

            if (remoteMaxOffset == null || MessageOffset.greateThan(currentOffset, remoteMaxOffset, isLong)) {
                delegator.putMaxOffset(shuffleId, window.getConfigureName(), oriQueueId, currentOffset);
            }
        }
    }


    @Override
    protected String generateShuffleKey(IMessage message) {
        return null;
    }

    /**
     * 根据message，把message分组到不同的group，分别处理
     *
     * @param messageList
     * @param instance2Messages
     * @param windowInstanceMap
     */
    protected void groupByWindowInstanceAndQueueId(List<IMessage> messageList,
                                                   Map<Pair<String, String>, List<IMessage>> instance2Messages, Map<String, WindowInstance> windowInstanceMap) {
        for (IMessage message : messageList) {
            //the queueId will be replace below, so get first here!
            String queueId = message.getHeader().getQueueId();
            String oriQueueId = message.getMessageBody().getString(WindowCache.ORIGIN_QUEUE_ID);
            String oriOffset = message.getMessageBody().getString(WindowCache.ORIGIN_OFFSET);
            Boolean isLong = message.getMessageBody().getBoolean(WindowCache.ORIGIN_QUEUE_IS_LONG);
            message.getHeader().setQueueId(oriQueueId);
            message.getHeader().setOffset(oriOffset);
            message.getHeader().setOffsetIsLong(isLong);
            List<WindowInstance> windowInstances = (List<WindowInstance>) message.getMessageBody().get(WindowInstance.class.getSimpleName());
            for (WindowInstance windowInstance : windowInstances) {
                String windowInstanceId = windowInstance.getWindowInstanceId();
                Pair<String, String> queueIdAndInstanceKey = Pair.of(queueId, windowInstanceId);
                List<IMessage> messages = instance2Messages.computeIfAbsent(queueIdAndInstanceKey, k -> new ArrayList<>());
                //in case of changing message concurrently in hop window
                IMessage cloneMessage = message.deepCopy();
                //bring window instance id into accumulator computation
                cloneMessage.getMessageBody().put("HIT_WINDOW_INSTANCE_ID", windowInstance.getWindowInstanceId());
                messages.add(cloneMessage);
                windowInstanceMap.put(windowInstanceId, windowInstance);
            }
        }
    }

}
