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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.AbstractStorage;
import org.apache.rocketmq.streams.window.storage.DataType;
import org.apache.rocketmq.streams.window.storage.IteratorWrap;
import org.apache.rocketmq.streams.window.storage.RocksdbIterator;
import org.apache.rocketmq.streams.window.storage.WindowJoinType;
import org.apache.rocketmq.streams.window.storage.WindowType;
import org.apache.rocketmq.streams.window.storage.rocksdb.RocksdbStorage;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.apache.rocketmq.streams.window.storage.DataType.WINDOW_INSTANCE;

public class DefaultStorage extends AbstractStorage {
    private final boolean isLocalStorageOnly;
    private final RocksdbStorage rocksdbStorage;


    //两个streams实例topic可能相同，但是tag不同
    private final String topic;
    private final String groupId;
    private final String tags;

    private DefaultMQProducer producer;
    private DefaultLitePullConsumer consumer;

    private static final long pollTimeoutMillis = 2 * 1000L;
    private Map<Integer, MessageQueue> queueId2MQ = new HashMap<>();
    private ExecutorService pollExecutor;


    public DefaultStorage(String topic, String groupId, String tags, String namesrv,
                          boolean isLocalStorageOnly, RocksdbStorage rocksdbStorage) {
        this.isLocalStorageOnly = isLocalStorageOnly;
        this.rocksdbStorage = rocksdbStorage;

        this.topic = topic;
        this.groupId = groupId;
        this.tags = tags;


        if (!isLocalStorageOnly) {
            this.pollExecutor = Executors.newSingleThreadExecutor();

            try {
                this.producer = new DefaultMQProducer(groupId);
                this.producer.setNamesrvAddr(namesrv);
                this.producer.start();

                this.consumer = new DefaultLitePullConsumer(this.groupId);
                this.consumer.setNamesrvAddr(namesrv);
                this.consumer.subscribe(topic, tags);
                this.consumer.setAutoCommit(false);
                this.consumer.start();
                //暂时认为MQ不会因为扩缩broker而扩缩，只在这里获取一次
                Collection<MessageQueue> mqs = this.consumer.fetchMessageQueues(topic);
                if (mqs != null) {
                    Map<Integer, List<MessageQueue>> temp = mqs.stream().collect(Collectors.groupingBy(MessageQueue::getQueueId));
                    for (Integer queueId : temp.keySet()) {
                        List<MessageQueue> messageQueues = temp.get(queueId);
                        assert messageQueues.size() == 1;
                        this.queueId2MQ.put(queueId, messageQueues.get(0));
                    }
                }

            } catch (Throwable t) {
                throw new RuntimeException("connect to rocketmq error.", t);
            }
        }
    }

    @Override
    public Future<?> load(String shuffleId) {
        MessageQueue messageQueue = getMessageQueue(shuffleId);
        if (messageQueue == null) {
            throw new RuntimeException("can not find MQ with shuffleId = [" + shuffleId + "]");
        }

        //从上一offset提交位置，poll到最新数据位置
        return this.pollExecutor.submit(() -> this.pollToLast(messageQueue));
    }

    private void pollToLast(MessageQueue messageQueue) {
        List<MessageQueue> temp = new ArrayList<>();
        temp.add(messageQueue);

        //assign 与poll必须原子；
        synchronized (this.consumer) {
            this.consumer.assign(temp);

            List<MessageExt> msgs = this.consumer.poll(pollTimeoutMillis);
            while (msgs.size() != 0) {
                this.replayState(msgs);
                msgs = this.consumer.poll(pollTimeoutMillis);
            }
        }

    }

    private void replayState(List<MessageExt> msgs) {
        if (msgs == null || msgs.size() == 0) {
            return;
        }

        //按照key进行分组；
        Map<String, List<MessageExt>> collect = msgs.stream().parallel().collect(Collectors.groupingBy(MessageExt::getKeys));


        //对每组key的所有msg的时间戳进行比较，过滤出最大时间戳的值,即为最后的状态
        HashMap<String, MessageExt> lastStates = new HashMap<>();
        collect.forEach((key, values) -> {

            long maxBornTimestamp = 0;
            MessageExt lastMsgExt = null;

            for (MessageExt msgExt : values) {
                long bornTimestamp = msgExt.getBornTimestamp();
                if (bornTimestamp > maxBornTimestamp) {
                    maxBornTimestamp = bornTimestamp;
                    lastMsgExt = msgExt;
                }
            }
            lastStates.put(key, lastMsgExt);
        });

        this.convert(lastStates);
    }

    private void convert(HashMap<String, MessageExt> lastStates) {
        for (String key : lastStates.keySet()) {
            MessageExt newState = lastStates.get(key);

            if (key.startsWith(WINDOW_INSTANCE.getValue())) {
                updateState(key, newState);
            } else if (key.startsWith(DataType.WINDOW_BASE_VALUE.getValue())) {
                updateState(key, newState);
            } else if (key.startsWith(DataType.MAX_OFFSET.getValue())) {
                updateState(key, newState);
            } else if (key.startsWith(DataType.MAX_PARTITION_NUM.getValue())) {
                updateState(key, newState);
            }
        }
    }


    private synchronized void updateState(String key, MessageExt newState) {
        byte[] body = newState.getBody();
        String result = SerializeUtil.deserialize2String(body);

        if (body == null) {
            return;
        }

        if (DeleteMessage.DELETE_MESSAGE.name().equals(result)) {
            rocksdbStorage.delete(key);
            return;
        }

        byte[] bytes = rocksdbStorage.get(key);
        if (bytes == null || bytes.length == 0) {
            rocksdbStorage.put(key, body);
            return;
        }

        Object newValue;
        if (result != null) {
            newValue = result;
            long newTimestamp = getTimestamp(newValue);


            String oldValue = new String(bytes, StandardCharsets.UTF_8);
            long oldTimestamp = getTimestamp(oldValue);

            if (newTimestamp > oldTimestamp) {
                rocksdbStorage.put(key, body);
            }

        } else {
            newValue = SerializeUtil.deserialize(body);

            if (newValue instanceof WindowBaseValue) {
                long newVersion = getTimestamp(newValue);

                WindowBaseValue oldValue = SerializeUtil.deserialize(bytes);
                long oldVersion = getTimestamp(oldValue);

                if (newVersion > oldVersion) {
                    rocksdbStorage.put(key, body);
                }
            }

            //windowInstance为窗口元数据，不存在更新的情况
        }
    }


    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {
        rocksdbStorage.putWindowInstance(shuffleId, windowNamespace, windowConfigureName, windowInstance);
    }

    @Override
    public <T> RocksdbIterator<T> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        return rocksdbStorage.getWindowInstance(shuffleId, windowNamespace, windowConfigureName);
    }

    //put的key是什么，就按照什么key删除
    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        rocksdbStorage.deleteWindowInstance(shuffleId, windowNamespace, windowConfigureName, windowInstanceKey);
    }


    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType,
                                   WindowJoinType joinType, List<WindowBaseValue> windowBaseValue) {
        rocksdbStorage.putWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType, windowBaseValue);
    }

    public void putWindowBaseValueIterator(String shuffleId, String windowInstanceId,
                                           WindowType windowType, WindowJoinType joinType,
                                           RocksdbIterator<? extends WindowBaseValue> windowBaseValueIterator) {
        rocksdbStorage.putWindowBaseValueIterator(shuffleId, windowInstanceId, windowType, joinType, windowBaseValueIterator);
    }

    //读取消息重放，或者查询并存储到内存
    @Override
    public <T> RocksdbIterator<T> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return rocksdbStorage.getWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType);
    }

    //按照put key的前缀删除，没有唯一键，删除一批
    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        rocksdbStorage.deleteWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType);
    }

    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType, String msgKey) {
        rocksdbStorage.deleteWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType, msgKey);
    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        return rocksdbStorage.getMaxOffset(shuffleId, windowConfigureName, oriQueueId);
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        rocksdbStorage.putMaxOffset(shuffleId, windowConfigureName, oriQueueId, offset);
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        rocksdbStorage.deleteMaxOffset(shuffleId, windowConfigureName, oriQueueId);
    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        rocksdbStorage.putMaxPartitionNum(shuffleId, windowInstanceKey, maxPartitionNum);
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        return rocksdbStorage.getMaxPartitionNum(shuffleId, windowInstanceKey);
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        rocksdbStorage.deleteMaxPartitionNum(shuffleId, windowInstanceKey);
    }


    //按照queueId提交offset，避免了不同streams实例，多次提交offset
    @Override
    public int flush(List<String> queueIdList) {
        if (isLocalStorageOnly) {
            return 0;
        }

        int successNum = 0;
        try {
            for (String queueId : queueIdList) {
                successNum += sendSync(queueId);
            }

            //提交上次checkpoint/load时，poll消息的offset
            for (String queueId : queueIdList) {
                final MessageQueue queue = getMessageQueue(queueId);
                //todo 指定messageQueue提交offset
                this.consumer.committed(queue);

                //poll到最新的checkpoint，为下一次提交offset做准备；
                this.pollExecutor.execute(() -> this.pollToLast(queue));
            }

        } catch (Throwable t) {
            throw new RuntimeException("send data to rocketmq synchronously，error.", t);
        }

        return successNum;
    }

    private int sendSync(String shuffleId) {
        int count = 0;

        for (DataType dataType : DataType.values()) {
            count += send(shuffleId, dataType);
        }

        return count;
    }

    private int send(String shuffleId, DataType dataType) {
        int count = 0;

        RocksdbIterator<Object> iterator = rocksdbStorage.getData(shuffleId, dataType);
        while (iterator.hasNext()) {
            IteratorWrap<Object> wrap = iterator.next();

            byte[] raw = wrap.getRaw();
            if (raw == null || raw.length ==0) {
                raw = DeleteMessage.DELETE_MESSAGE.name().getBytes(StandardCharsets.UTF_8);
            }

            count += send0(shuffleId, wrap.getKey(), raw);
        }

        return count;
    }


    private int send0(String shuffleId, String key, byte[] body) {
        MessageQueue queue = getMessageQueue(shuffleId);
        try {

            Message message = new Message(topic, tags, key, body);
            //todo 没报错是否就是send_ok，选择MQ写入，后面commitOffset时对这个MQ进行
            producer.send(message, queue);

            return 1;
        } catch (Throwable t) {
            throw new RuntimeException("send data to rocketmq asynchronously，error.", t);
        }
    }

    //状态topic的MQ数量与shuffle topic的MQ数量需要相同
    private MessageQueue getMessageQueue(String shuffleId) {
        //最后四位为queueId
        String substring = shuffleId.substring(shuffleId.length() - 4);
        Integer queueIdNumber = Integer.parseInt(substring);
        return queueId2MQ.get(queueIdNumber);
    }


    enum DeleteMessage {
        DELETE_MESSAGE
    }

}
