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

import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class RocksDBStore extends AbstractStore {
    private static final String ROCKSDB_PATH = "/tmp/rocksdb";
    private RocksDB rocksDB;
    private WriteOptions writeOptions;

    protected final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<Object/*Key*/>> stateTopicQueue2RocksDBKey = new ConcurrentHashMap<>();

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

    @Override
    public synchronized void init() throws Throwable {
        synchronized (lock) {
            if (state == StoreState.UNINITIALIZED) {
                synchronized (lock) {
                    createRocksDB();
                    state = StoreState.INITIALIZED;
                }
            }
        }
    }

    @Override
    public void waitIfNotReady(MessageQueue messageQueue, Object key) {
        if (state != StoreState.INITIALIZED) {
            throw new RuntimeException("RocksDB not ready.");
        }

        MessageQueue stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(messageQueue);
        String stateTopicQueueKey = buildKey(stateTopicQueue);

        Set<Object> keySet = this.stateTopicQueue2RocksDBKey.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
        keySet.add(key);
    }


    public void removeState(Set<MessageQueue> removeQueues) throws Throwable {
        if (removeQueues == null || removeQueues.size() == 0) {
            return;
        }
        Set<MessageQueue> stateTopicQueue = convertSourceTopicQueue2StateTopicQueue(removeQueues);

        Map<String/*brokerName@topic@queueId*/, List<MessageQueue>> groupByUniqueQueue = stateTopicQueue.stream().parallel().collect(Collectors.groupingBy(this::buildKey));

        Set<String> uniqueQueues = groupByUniqueQueue.keySet();
        for (String uniqueQueue : uniqueQueues) {
            Set<Object> keys = this.stateTopicQueue2RocksDBKey.get(uniqueQueue);
            if (keys == null) {
                continue;
            }

            for (Object key : keys) {
                byte[] keyBytes = this.object2Byte(key);
                this.rocksDB.delete(keyBytes);
            }
            this.stateTopicQueue2RocksDBKey.remove(uniqueQueue);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> V get(K key) {
        if (key == null) {
            return null;
        }

        try {
            byte[] bytes = this.object2Byte(key);
            byte[] valueBytes = rocksDB.get(bytes);

            if (valueBytes == null || valueBytes.length == 0) {
                return null;
            }

            return (V) byte2Object(valueBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <K, V> void put(K key, V value) {
        try {
            byte[] keyBytes = this.object2Byte(key);

            byte[] valueBytes = this.object2Byte(value);

            rocksDB.put(writeOptions, keyBytes, valueBytes);
        } catch (Exception e) {
            throw new RuntimeException("putWindowInstance to rocksdb error", e);
        }
    }

    @Override
    public void persist(Set<MessageQueue> messageQueue) throws Throwable {

    }

    @Override
    public void close() throws Exception {

    }

    ConcurrentHashMap<String, Set<Object>> getStateTopicQueue2RocksDBKey() {
        return stateTopicQueue2RocksDBKey;
    }
}
