package org.apache.rocketmq.streams.window.storage.rocketmq;
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

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.apache.rocketmq.streams.window.storage.AbstractStorage;
import org.apache.rocketmq.streams.window.storage.IStorage;
import org.apache.rocketmq.streams.window.storage.SendStateCallBack;
import org.apache.rocketmq.streams.window.storage.WindowJoinType;
import org.apache.rocketmq.streams.window.storage.WindowType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RocketmqKV extends AbstractStorage {
    private static final String SEND_TIMESTAMP = "sendTimestamp";
    private boolean isLocalStorageOnly;
    private DefaultMQProducer producer;
    private DefaultMQPushConsumer consumer;
    //两个streams实例topic可能相同，但是tag不同
    private String topic;
    private String tags;

    private ExecutorService executorService;
    //发送失败message
    private static final long maxRetain = 100_000L;
    private final AtomicLong currentRetain = new AtomicLong(0);
    private final ConcurrentHashMap<String, List<Message>> sendFailed = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Wrap<WindowInstance>> windowInstanceStates = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Wrap<WindowBaseValue>> windowBaseValueStates = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Wrap<String>> maxOffsetStates = new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, Wrap<String>> maxPartitionNumStates = new ConcurrentHashMap<>();

    public RocketmqKV(String topic, String group, String tags, String namesrv, boolean isLocalStorageOnly) {
        this.isLocalStorageOnly = isLocalStorageOnly;

        if (!isLocalStorageOnly) {
            //todo 有限队列
            this.executorService = Executors.newFixedThreadPool(4);

            this.topic = topic;
            this.tags = tags;
            try {
                this.producer = new DefaultMQProducer(group);
                this.producer.setNamesrvAddr(namesrv);
                this.producer.start();

                //todo 需要使用pull consumer 指定位点拉取消息；在做checkout时需要提交位点；
                this.consumer = new DefaultMQPushConsumer(group);
                this.consumer.setNamesrvAddr(namesrv);
                this.consumer.subscribe(topic, tags);
                this.consumer.registerMessageListener((MessageListenerConcurrently) (msgs, context) -> {
                    replayState(msgs);
                    return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                });

                this.consumer.start();
            } catch (Throwable t) {

            }
        }

    }

    private void replayState(List<MessageExt> msgs) {
        if (msgs == null || msgs.size() == 0) {
            return;
        }

        //按照key进行分组；
        Map<String, List<MessageExt>> collect = msgs.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));


        //对每组key的所有msg的时间戳进行比较，过滤出最大时间戳的值
        HashMap<String, MessageExt> lastStates = new HashMap<>();
        collect.forEach((key, values) -> {

            long maxSendTimestamp = 0;
            MessageExt lastMsgExt = null;

            for (MessageExt msgExt : values) {
                long sendTimestamp = getSendTimestamp(msgExt);
                if (sendTimestamp > maxSendTimestamp) {
                    maxSendTimestamp = sendTimestamp;
                    lastMsgExt = msgExt;
                }
            }
            lastStates.put(key, lastMsgExt);
        });

        executorService.execute(() -> this.convert(lastStates));
    }

    private void convert(HashMap<String, MessageExt> lastStates) {
        for (String key : lastStates.keySet()) {
            MessageExt newState = lastStates.get(key);

            if (key.startsWith(KeyPrefix.WINDOW_INSTANCE.value)) {
                updateState(key, newState, windowInstanceStates);
            } else if (key.startsWith(KeyPrefix.WINDOW_BASE_VALUE.value)) {
                updateState(key, newState, windowBaseValueStates);
            } else if (key.startsWith(KeyPrefix.MAX_OFFSET.value)) {
                updateState(key, newState, maxOffsetStates);
            } else if (key.startsWith(KeyPrefix.MAX_PARTITION_NUM.value)) {
                updateState(key, newState, maxPartitionNumStates);
            }


        }
    }

    @SuppressWarnings("unchecked")
    private <T> void updateState(String key, MessageExt newState, final ConcurrentHashMap<String, Wrap<T>> target) {
        long sendTimestamp = getSendTimestamp(newState);
        Object obj = SerializeUtil.deserialize(newState.getBody());

        if (obj instanceof DeleteMessage) {
            target.remove(key);
        } else {
            T value = (T) obj;

            synchronized (target) {
                Wrap<T> wrap = target.get(key);

                if (wrap == null || wrap.sendTimestamp < sendTimestamp) {
                    Wrap<T> temp = new Wrap<>(sendTimestamp, value);
                    target.put(key, temp);
                }
            }
        }

    }


    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {
        String key = super.buildKey(KeyPrefix.WINDOW_INSTANCE.value, shuffleId, windowNamespace,
                windowConfigureName, windowInstance.getWindowInstanceKey());

        byte[] value = SerializeUtil.serialize(windowInstance);

        //提前放入内存
        windowInstanceStates.put(key, new Wrap<>(System.currentTimeMillis(), windowInstance));
        sendIfNecessary(key, value, shuffleId);
    }

    @Override
    public List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {

        String keyPrefix = super.buildKey(KeyPrefix.WINDOW_INSTANCE.value, shuffleId, windowNamespace, windowConfigureName);

        List<WindowInstance> result = new ArrayList<>();
        for (String key : windowInstanceStates.keySet()) {
            if (key.startsWith(keyPrefix)) {
                result.add(windowInstanceStates.get(key).data);
            }
        }
        return result;
    }

    //put的key是什么，就按照什么key删除
    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        String key = super.buildKey(KeyPrefix.WINDOW_INSTANCE.value, shuffleId, windowNamespace, windowConfigureName, windowInstanceKey);

        windowInstanceStates.remove(key);
        sendIfNecessary(key, DeleteMessage.DELETE_MESSAGE.name(), shuffleId);
    }


    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType,
                                   WindowJoinType joinType, List<WindowBaseValue> windowBaseValue) {
        if (windowBaseValue == null || windowBaseValue.size() == 0) {
            return;
        }

        for (WindowBaseValue baseValue : windowBaseValue) {
            String key;
            switch (windowType) {
                case SESSION_WINDOW:
                case NORMAL_WINDOW: {
                    key = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name(), baseValue.getMsgKey());
                    break;
                }
                case JOIN_WINDOW: {
                    JoinState joinState = (JoinState) baseValue;
                    key = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name(), joinType.name(), joinState.getMessageId());
                    break;
                }
                default:
                    throw new RuntimeException("windowType " + windowType + "illegal.");
            }

            byte[] value = SerializeUtil.serialize(baseValue);

            windowBaseValueStates.put(key, new Wrap<>(System.currentTimeMillis(), baseValue));
            sendIfNecessary(key, value, shuffleId);
        }
    }

    //读取消息重放，或者查询并存储到内存
    @Override
    public List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        String keyPrefix;
        switch (windowType) {
            case SESSION_WINDOW:
            case NORMAL_WINDOW: {
                keyPrefix = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name());
                break;
            }
            case JOIN_WINDOW: {
                keyPrefix = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name(), joinType.name());
                break;
            }
            default:
                throw new RuntimeException("windowType " + windowType + "illegal.");
        }

        ArrayList<WindowBaseValue> result = new ArrayList<>();
        for (String key : windowBaseValueStates.keySet()) {
            if (key.startsWith(keyPrefix)) {
                result.add(windowBaseValueStates.get(key).data);
            }
        }

        return result;
    }

    //按照put key的前缀删除，没有唯一键，删除一批
    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        String keyPrefix;
        switch (windowType) {
            case SESSION_WINDOW:
            case NORMAL_WINDOW: {
                keyPrefix = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name());
                break;
            }
            case JOIN_WINDOW: {
                keyPrefix = super.buildKey(KeyPrefix.WINDOW_BASE_VALUE.value, shuffleId, windowInstanceId, windowType.name(), joinType.name());
                break;
            }
            default:
                throw new RuntimeException("windowType " + windowType + "illegal.");
        }

        //先从内存中找出完整的key
        ArrayList<String> keys = new ArrayList<>();
        for (String key : windowBaseValueStates.keySet()) {
            if (key.startsWith(keyPrefix)) {
                keys.add(key);
            }
        }


        for (String key : keys) {
            windowBaseValueStates.remove(key);
            sendIfNecessary(key, DeleteMessage.DELETE_MESSAGE.name(), shuffleId);
        }

    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = super.buildKey(KeyPrefix.MAX_OFFSET.value, shuffleId, windowConfigureName, oriQueueId);

        Wrap<String> wrap = this.maxOffsetStates.get(key);
        if (wrap == null) {
            return null;
        } else {
            return wrap.data;
        }
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        String key = super.buildKey(KeyPrefix.MAX_OFFSET.value, shuffleId, windowConfigureName, oriQueueId);

        maxOffsetStates.put(key, new Wrap<>(System.currentTimeMillis(), offset));
        sendIfNecessary(key, offset, shuffleId);
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = super.buildKey(KeyPrefix.MAX_OFFSET.value, shuffleId, windowConfigureName, oriQueueId);

        maxOffsetStates.remove(key);
        sendIfNecessary(key, DeleteMessage.DELETE_MESSAGE.name(), shuffleId);
    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        String key = super.buildKey(KeyPrefix.MAX_PARTITION_NUM.value, shuffleId, windowInstanceKey);

        maxPartitionNumStates.put(key, new Wrap<>(System.currentTimeMillis(), String.valueOf(maxPartitionNum)));
        sendIfNecessary(key, String.valueOf(maxPartitionNum), shuffleId);
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = super.buildKey(KeyPrefix.MAX_PARTITION_NUM.value, shuffleId, windowInstanceKey);

        Wrap<String> wrap = this.maxPartitionNumStates.get(key);
        if (wrap == null) {
            return null;
        } else {
            return Long.parseLong(wrap.data);
        }
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = super.buildKey(KeyPrefix.MAX_PARTITION_NUM.value, shuffleId, windowInstanceKey);

        maxPartitionNumStates.remove(key);
        sendIfNecessary(key, DeleteMessage.DELETE_MESSAGE.name(), shuffleId);
    }

    @Override
    public void clearCache(String queueId) {
        removeByKey(windowInstanceStates, queueId);
        removeByKey(windowBaseValueStates, queueId);
        removeByKey(maxOffsetStates, queueId);
        removeByKey(maxPartitionNumStates, queueId);
    }


    @Override
    public int flush(List<String> queueIdList) {
        if (isLocalStorageOnly) {
            return 0;
        }

        int successNum = 0;
        try {
            for (String queueId : queueIdList) {
                List<Message> messageList = this.sendFailed.get(queueId);
                if (messageList == null) {
                    this.sendFailed.remove(queueId);
                    continue;
                }

                Iterator<Message> iterator = messageList.iterator();
                while (iterator.hasNext()) {
                    Message message = iterator.next();
                    SendResult result = this.producer.send(message);
                    if (result.getSendStatus() == SendStatus.SEND_OK) {

                        synchronized (this.currentRetain) {
                            iterator.remove();
                            this.currentRetain.decrementAndGet();
                            successNum++;
                        }
                    }
                }

            }
        } catch (Throwable t) {

        }

        return successNum;
    }

    private void sendIfNecessary(String key, Object body, String shuffleId) {
        if (isLocalStorageOnly) {
            return;
        }

        try {
            Message message;
            if (body instanceof String) {
                message = new Message(topic, tags, key, ((String) body).getBytes(StandardCharsets.UTF_8));
            } else if (body instanceof byte[]) {
                message = new Message(topic, tags, key, (byte[]) body);
            } else {
                throw new UnsupportedOperationException();
            }

            message.putUserProperty(SEND_TIMESTAMP, String.valueOf(System.currentTimeMillis()));
            producer.send(message, new SendStateCallBack(shuffleId, message, currentRetain, maxRetain, sendFailed));
        } catch (Throwable t) {

        }
    }

    private long getSendTimestamp(MessageExt msgExt) {
        String userProperty = msgExt.getUserProperty(SEND_TIMESTAMP);
        if (userProperty == null) {
            return 0L;
        }

        return Long.parseLong(userProperty);
    }

    private <T> void removeByKey(ConcurrentHashMap<String, Wrap<T>> source, String shuffleId) {
        Set<Map.Entry<String, Wrap<T>>> windowInstanceEntrySet = source.entrySet();

        for (Map.Entry<String, Wrap<T>> next : windowInstanceEntrySet) {
            String key = next.getKey();
            if (findShuffleId(key, shuffleId)) {
                source.remove(key);
            }
        }
    }

    private boolean findShuffleId(String key, String shuffleId) {
        String[] split = key.split(IStorage.SEPARATOR);
        return split[1].equals(shuffleId);
    }

    enum KeyPrefix {
        WINDOW_INSTANCE("windowInstance" + IStorage.SEPARATOR),
        WINDOW_BASE_VALUE("windowBaseValue" + IStorage.SEPARATOR),
        MAX_OFFSET("maxOffset" + IStorage.SEPARATOR),
        MAX_PARTITION_NUM("maxPartitionNum" + IStorage.SEPARATOR);

        private final String value;

        KeyPrefix(String value) {
            this.value = value;
        }
    }

    enum DeleteMessage {
        DELETE_MESSAGE
    }

    static class Wrap<T> {
        private final long sendTimestamp;
        private final T data;

        public Wrap(long sendTimestamp, T data) {
            this.sendTimestamp = sendTimestamp;
            this.data = data;
        }
    }

}
