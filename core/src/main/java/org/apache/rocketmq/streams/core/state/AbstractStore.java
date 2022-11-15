package org.apache.rocketmq.streams.core.state;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractStore {
    private final Wrapper wrapper = new Wrapper();


    protected void putInRecover(String stateTopicQueueKey, byte[] key) {
        wrapper.putInRecover(stateTopicQueueKey, key);
    }

    protected void putInCalculating(String stateTopicQueueKey, byte[] key) {
        wrapper.putInCalculating(stateTopicQueueKey, key);
    }

    protected Set<byte[]> getInCalculating(String stateTopicQueue) {
        return wrapper.getInCalculating(stateTopicQueue);
    }

    protected void removeCalculating(String stateTopicQueue) {
        wrapper.removeCalculating(stateTopicQueue);
    }

    protected Set<byte[]> getAll(String stateTopicQueue) {
        return wrapper.getAll(stateTopicQueue);
    }


    protected String whichStateTopicQueueBelongTo(byte[] key) {
        return wrapper.whichStateTopicQueueBelongTo(key);
    }

    protected void removeAllKey(byte[] key) {
        wrapper.deleteByKey(key);
    }


    protected void removeAll(String stateTopicQueue) {
        wrapper.removeAll(stateTopicQueue);
    }

    protected MessageQueue convertSourceTopicQueue2StateTopicQueue(MessageQueue messageQueue) {
        HashSet<MessageQueue> messageQueues = new HashSet<>();
        messageQueues.add(messageQueue);

        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(messageQueues);

        Iterator<MessageQueue> iterator = stateTopicQueue.iterator();
        return iterator.next();
    }

    protected Set<MessageQueue> convertSourceTopicQueue2StateTopicQueue(Set<MessageQueue> messageQueues) {
        if (messageQueues == null || messageQueues.size() == 0) {
            return new HashSet<>();
        }

        HashSet<MessageQueue> result = new HashSet<>();
        for (MessageQueue messageQueue : messageQueues) {
            if (messageQueue.getTopic().endsWith(Constant.STATE_TOPIC_SUFFIX)) {
                result.add(messageQueue);
                continue;
            }
            MessageQueue queue = new MessageQueue(messageQueue.getTopic() + Constant.STATE_TOPIC_SUFFIX, messageQueue.getBrokerName(), messageQueue.getQueueId());
            result.add(queue);
        }

        return result;
    }

    protected static String stateTopic2SourceTopic(String stateTopic) {
        if (StringUtils.isEmpty(stateTopic)) {
            return null;
        }

        return stateTopic.substring(0, stateTopic.lastIndexOf(Constant.STATE_TOPIC_SUFFIX));
    }


    protected String buildKey(MessageExt messageExt) {
        return Utils.buildKey(messageExt.getBrokerName(), messageExt.getTopic(), messageExt.getQueueId());
    }

    protected String buildKey(MessageQueue messageQueue) {
        return Utils.buildKey(messageQueue.getBrokerName(), messageQueue.getTopic(), messageQueue.getQueueId());
    }

    static class Wrapper {
        //新增，写消费未提交保存的中间状态，提交时移除
        private final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<byte[]/*Key*/>> calculating = new ConcurrentHashMap<>();
        //全量, 与rocksdb报错同步
        private final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<byte[]/*Key*/>> recover = new ConcurrentHashMap<>();

        public void putInRecover(String stateTopicQueueKey, byte[] key) {
            Set<byte[]> allSet = this.recover.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
            allSet.add(key);
        }

        public void putInCalculating(String stateTopicQueueKey, byte[] key) {
            Set<byte[]> keySet = this.calculating.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
            keySet.add(key);

            putInRecover(stateTopicQueueKey, key);
        }

        public Set<byte[]> getInCalculating(String stateTopicQueue) {
            return calculating.get(stateTopicQueue);
        }

        public Set<byte[]> getAll(String stateTopicQueue) {
            Set<byte[]> calculating = this.calculating.get(stateTopicQueue);
            Set<byte[]> recover = this.recover.get(stateTopicQueue);

            Set<byte[]> result = new HashSet<>();
            result.addAll(calculating);
            result.addAll(recover);

            //可能有重复，不同byte[] 但是时一个key
            return result;
        }

        public String whichStateTopicQueueBelongTo(byte[] key) {
            for (String uniqueQueue : recover.keySet()) {
                for (byte[] tempKeyByte : recover.get(uniqueQueue)) {
                    if (Arrays.equals(tempKeyByte, key)) {
                        return uniqueQueue;
                    }
                }
            }

            for (String uniqueQueue : calculating.keySet()) {
                for (byte[] tempKeyByte : calculating.get(uniqueQueue)) {
                    if (Arrays.equals(tempKeyByte, key)) {
                        return uniqueQueue;
                    }
                }
            }

            return null;
        }


        public void deleteByKey(byte[] key) {
            {
                Set<Map.Entry<String, Set<byte[]>>> entries = calculating.entrySet();
                Iterator<Map.Entry<String, Set<byte[]>>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Set<byte[]>> next = iterator.next();

                    Set<byte[]> keySet = next.getValue();
                    keySet.removeIf(rocksDBKey -> Arrays.equals(rocksDBKey, key));
                    if (keySet.size() == 0) {
                        iterator.remove();
                    }
                }
            }

            {
                Set<Map.Entry<String, Set<byte[]>>> entries = recover.entrySet();
                Iterator<Map.Entry<String, Set<byte[]>>> iterator = entries.iterator();
                while (iterator.hasNext()) {
                    Map.Entry<String, Set<byte[]>> next = iterator.next();

                    Set<byte[]> keySet = next.getValue();
                    keySet.removeIf(rocksDBKey -> Arrays.equals(rocksDBKey, key));
                    if (keySet.size() == 0) {
                        iterator.remove();
                    }
                }
            }



        }

        public void removeCalculating(String stateTopicQueueKey) {
            this.calculating.remove(stateTopicQueueKey);
        }

        public void removeAll(String stateTopicQueueKey) {
            this.recover.remove(stateTopicQueueKey);
            this.calculating.remove(stateTopicQueueKey);
        }

    }
}
