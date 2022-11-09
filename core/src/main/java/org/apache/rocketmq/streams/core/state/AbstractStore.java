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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public abstract class AbstractStore {
    private final Wrapper wrapper = new Wrapper();

    public <V> V byte2Object(byte[] bytes, Class<V> clazz) throws IOException {
        return Utils.byte2Object(bytes, clazz);
    }

    public byte[] object2Bytes(Object target) throws JsonProcessingException {
        return Utils.object2Byte(target);
    }

    @SuppressWarnings("unchecked")
    protected <K, V> Pair<Class<K>, Class<V>> getClazz(K key) {
        Pair<Class<?>, Class<?>> clazzPair = wrapper.getClazz(key);

        return new Pair<>((Class<K>) clazzPair.getObject1(), (Class<V>) clazzPair.getObject2());
    }

    protected <K, V> void putClazz(String stateTopicQueueKey, K key, V value) {
        wrapper.put(stateTopicQueueKey, key, value);
    }

    protected Set<Object> whichKeyMap2StateTopicQueue(String stateTopicQueue) {
        return wrapper.whichKeyMap2StateTopicQueue(stateTopicQueue);
    }

    protected String whichStateTopicQueueBelongTo(Object key) {
        return wrapper.whichStateTopicQueueBelongTo(key);
    }

    protected <K> void deleteAllMappingByKey(K key) {
        wrapper.deleteByKey(key);
    }

    protected void deleteAllMappingByStateTopicQueue(String stateTopicQueueKey) {
        wrapper.deleteByStateQueue(stateTopicQueueKey);
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
        private final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<Object/*Key*/>> stateTopicQueue2Key = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<Object/*Key*/, Pair<Class<?>/*Key class*/, Class<?>/*value class*/>> key2Clazz = new ConcurrentHashMap<>();

        public void put(String stateTopicQueueKey, Object key, Object value) {
            Set<Object> keySet = this.stateTopicQueue2Key.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
            keySet.add(key);

            if (!key2Clazz.containsKey(key)) {
                this.key2Clazz.put(key, new Pair<>(key.getClass(), value.getClass()));
            }
        }

        public Set<Object> whichKeyMap2StateTopicQueue(String stateTopicQueue) {
            return stateTopicQueue2Key.get(stateTopicQueue);
        }

        public Pair<Class<?>, Class<?>> getClazz(Object key) {
            return key2Clazz.get(key);
        }

        public String whichStateTopicQueueBelongTo(Object key) {
            for (String uniqueQueue : stateTopicQueue2Key.keySet()) {
                if (stateTopicQueue2Key.get(uniqueQueue).contains(key)) {
                    return uniqueQueue;
                }
            }

            return null;
        }

        public Object getKeyObject(String stateTopicQueueKey) {
            return stateTopicQueue2Key.get(stateTopicQueueKey);
        }

        public void deleteByKey(Object key) {
            //删除 stateTopicQueue2Key
            Set<Map.Entry<String, Set<Object>>> entries = stateTopicQueue2Key.entrySet();
            Iterator<Map.Entry<String, Set<Object>>> iterator = entries.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Set<Object>> next = iterator.next();

                Set<Object> keySet = next.getValue();
                Iterator<Object> keySetIterator = keySet.iterator();
                while (keySetIterator.hasNext()) {
                    Object rocksDBKey = keySetIterator.next();
                    if (rocksDBKey.equals(key)) {
                         keySetIterator.remove();
                    }
                }
                if (keySet.size() == 0) {
                    iterator.remove();
                }
            }

            //删除key2Clazz
            key2Clazz.remove(key);
        }

        public void deleteByStateQueue(String uniqueQueue) {
            Set<Object> remove = this.stateTopicQueue2Key.remove(uniqueQueue);
            for (Object obj : remove) {
                this.key2Clazz.remove(obj);
            }
        }
    }
}
