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


import com.google.common.collect.ImmutableMap;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RocksDBStore extends AbstractStore {
    private static final String ROCKSDB_PATH = "/tmp/rocksdb";
    private RocksDB rocksDB;
    private WriteOptions writeOptions;

    protected final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<RocksDBKey/*Key*/>> stateTopicQueue2RocksDBKey = new ConcurrentHashMap<>();

    public RocksDBStore() {
        createRocksDB();
    }

    private void createRocksDB() {
        try (final Options options = new Options().setCreateIfMissing(true)) {

            try {
                String localAddress = RemotingUtil.getLocalAddress();
                int pid = UtilAll.getPid();

                String rocksdbFilePath = String.format("%s/%s/%s", ROCKSDB_PATH, localAddress, pid);


                File dir = new File(rocksdbFilePath);
                if (dir.exists() && !dir.delete()) {
                    throw new RuntimeException("before create rocksdb, delete exist path " + rocksdbFilePath + " error");
                }

                if (!dir.mkdirs()) {
                    throw new RuntimeException("before create rocksdb,mkdir path " + rocksdbFilePath + " error");
                }

                this.rocksDB = TtlDB.open(options, rocksdbFilePath, 10800, false);

                writeOptions = new WriteOptions();
                writeOptions.setSync(false);
                writeOptions.setDisableWAL(true);
            } catch (RocksDBException e) {
                throw new RuntimeException("create rocksdb error " + e.getMessage());
            }
        }
    }


    public void removeState(Set<MessageQueue> removeQueues) throws Throwable {
        if (removeQueues == null || removeQueues.size() == 0) {
            return;
        }
        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(removeQueues);

        Map<String/*brokerName@topic@queueId*/, List<MessageQueue>> groupByUniqueQueue = stateTopicQueue.stream().parallel().collect(Collectors.groupingBy(this::buildKey));

        Set<String> uniqueQueues = groupByUniqueQueue.keySet();
        for (String uniqueQueue : uniqueQueues) {
            Set<RocksDBKey> keys = this.stateTopicQueue2RocksDBKey.get(uniqueQueue);
            if (keys == null) {
                continue;
            }

            for (RocksDBKey key : keys) {
                byte[] keyBytes = Utils.object2Byte(key.getKeyObject());
                this.rocksDB.delete(keyBytes);
            }
            this.stateTopicQueue2RocksDBKey.remove(uniqueQueue);
        }
    }

    public <K, V> V get(K key) {
        if (key == null) {
            return null;
        }

        try {
            byte[] bytes;
            if (key instanceof byte[]) {
                bytes = (byte[]) key;
            } else {
                bytes = Utils.object2Byte(key);
            }

            byte[] valueBytes = rocksDB.get(bytes);

            if (valueBytes == null || valueBytes.length == 0) {
                return null;
            }

            Pair<Class<K>, Class<V>> classPair = getValueClazz(key);

            return Utils.byte2Object(valueBytes, classPair.getObject2());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public <K, V> void put(MessageQueue stateTopicMessageQueue, K key, V value) {
        try {
            byte[] keyBytes;
            if (key instanceof byte[]) {
                keyBytes = (byte[]) key;
            } else {
                keyBytes = Utils.object2Byte(key);
            }

            byte[] valueBytes;
            if (value instanceof byte[]) {
                valueBytes = (byte[]) value;
            } else {
                valueBytes = Utils.object2Byte(value);
            }

            rocksDB.put(writeOptions, keyBytes, valueBytes);

            String stateTopicQueueKey = buildKey(stateTopicMessageQueue);

            Set<RocksDBKey> keySet = this.stateTopicQueue2RocksDBKey.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
            keySet.add(new RocksDBKey(key, key.getClass(), value.getClass()));
        } catch (Exception e) {
            throw new RuntimeException("putWindowInstance to rocksdb error", e);
        }
    }

    public <K> void deleteByKey(K key) throws RocksDBException {
        byte[] keyBytes = Utils.object2Byte(key);
        rocksDB.delete(keyBytes);

        //从内存缓存中删除待持久化的key
        Set<Map.Entry<String, Set<RocksDBKey>>> entries = stateTopicQueue2RocksDBKey.entrySet();
        Iterator<Map.Entry<String, Set<RocksDBKey>>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<RocksDBKey>> next = iterator.next();

            Set<RocksDBKey> keySet = next.getValue();
            for (RocksDBKey rocksDBKey : keySet) {
                if (rocksDBKey.getKeyObject().equals(key)) {
                    keySet.remove(rocksDBKey);
                }
            }
            if (keySet.size() == 0) {
                iterator.remove();
            }
        }
    }


    public RocksDB getRocksDB() {
        return this.rocksDB;
    }

    public void close() throws Exception {

    }

    Map<String, Set<RocksDBKey>> getStateTopicQueue2RocksDBKey() {
        return ImmutableMap.copyOf(stateTopicQueue2RocksDBKey);
    }


    private <K, V> Pair<Class<K>, Class<V>> getValueClazz(K key) {
        Collection<Set<RocksDBKey>> values = stateTopicQueue2RocksDBKey.values();
        for (Set<RocksDBKey> value : values) {
            for (RocksDBKey rocksDBKey : value) {
                if (rocksDBKey.getKeyObject().equals(key)) {
                    return new Pair<>(rocksDBKey.getKeyClazz(), rocksDBKey.getValueClazz());
                }
            }
        }

        throw new IllegalStateException("get key class and value class with key=[" + key + "], but empty ");
    }
}
