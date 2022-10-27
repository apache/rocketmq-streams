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

    protected final ConcurrentHashMap<String/*brokerName@topic@queueId of state topic*/, Set<Object/*Key*/>> stateTopicQueue2RocksDBKey = new ConcurrentHashMap<>();

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
            Set<Object> keys = this.stateTopicQueue2RocksDBKey.get(uniqueQueue);
            if (keys == null) {
                continue;
            }

            for (Object key : keys) {
                byte[] keyBytes = Utils.object2Byte(key);
                this.rocksDB.delete(keyBytes);
            }
            this.stateTopicQueue2RocksDBKey.remove(uniqueQueue);
        }
    }

    @SuppressWarnings("unchecked")
    public <K, V> V get(K key) {
        if (key == null) {
            return null;
        }

        try {
            byte[] bytes = Utils.object2Byte(key);
            byte[] valueBytes = rocksDB.get(bytes);

            if (valueBytes == null || valueBytes.length == 0) {
                return null;
            }

            return (V) Utils.byte2Object(valueBytes);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public <K, V> void put(MessageQueue stateTopicMessageQueue, K key, V value) {
        try {
            byte[] keyBytes = Utils.object2Byte(key);

            byte[] valueBytes = Utils.object2Byte(value);

            rocksDB.put(writeOptions, keyBytes, valueBytes);

            String stateTopicQueueKey = buildKey(stateTopicMessageQueue);

            Set<Object> keySet = this.stateTopicQueue2RocksDBKey.computeIfAbsent(stateTopicQueueKey, s -> new HashSet<>());
            keySet.add(key);
        } catch (Exception e) {
            throw new RuntimeException("putWindowInstance to rocksdb error", e);
        }
    }

    public <K> void deleteByKey(K key) throws RocksDBException {
        byte[] keyBytes = Utils.object2Byte(key);
        rocksDB.delete(keyBytes);

        //从内存缓存中删除待持久化的key
        Set<Map.Entry<String, Set<Object>>> entries = stateTopicQueue2RocksDBKey.entrySet();
        Iterator<Map.Entry<String, Set<Object>>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Set<Object>> next = iterator.next();

            Set<Object> keySet = next.getValue();
            keySet.remove(key);
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

    Map<String, Set<Object>> getStateTopicQueue2RocksDBKey() {
        return ImmutableMap.copyOf(stateTopicQueue2RocksDBKey);
    }


    public static void main(String[] args) throws Throwable {
        RocksDBStore rocksDBStore = new RocksDBStore();


        String key = "ksldk@1666850400000@5";

        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), "ksldk@1666850200000@9", "2");
        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), key, "4");
        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), "ksldk@1666850600000@2", "6");
        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), "ksldk@1666850300000@7", "3");
        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), "ksldk@1666850500000@1", "5");


        rocksDBStore.put(new MessageQueue("topic", "defalut", 1), "ksldk@1666850700000@6", "7");
        Object o = rocksDBStore.get(key);
        System.out.println(o);

        String keyPrefix = "ksldk@1666850500000";
        byte[] bytes = Utils.object2Byte(keyPrefix);
        Object o1 = rocksDBStore.searchByKeyPrefix(bytes);
        System.out.println(o1);
    }

    public List<byte[]> searchByKeyPrefix(byte[] keyPrefix) {
        RocksIterator newIterator = this.rocksDB.newIterator();
        newIterator.seekForPrev(keyPrefix);

        ArrayList<Pair<?,?>> temp = new ArrayList<>();
        while (newIterator.isValid()) {
            byte[] keyBytes = newIterator.key();
            byte[] valueBytes = newIterator.value();

            String key = Utils.byte2Object(keyBytes);
            String value = Utils.byte2Object(valueBytes);

            temp.add(new Pair<>(key, value));

            newIterator.prev();
        }

        for (Pair o : temp) {
            System.out.println(o.getObject1());
        }

        return null;
    }
}
