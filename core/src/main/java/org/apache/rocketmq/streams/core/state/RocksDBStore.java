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
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.remoting.common.RemotingUtil;
import org.apache.rocketmq.streams.core.runtime.operators.SessionWindowState;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteOptions;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class RocksDBStore extends AbstractStore {
    private static final String ROCKSDB_PATH = "/tmp/rocksdb";
    private RocksDB rocksDB;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;

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


    public byte[] get(byte[] key) throws RocksDBException {
        if (key == null) {
            return null;
        }

        return rocksDB.get(key);
    }


    public void put(byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(writeOptions, key, value);
    }

    public <V> List<Pair<String, V>> searchLessThanKeyPrefix(String keyPrefix, Class<V> valueClazz) throws IOException {
        byte[] keyPrefixBytes = super.object2Bytes(keyPrefix);
        readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
        RocksIterator rocksIterator = rocksDB.newIterator(readOptions);

        rocksIterator.seekForPrev(keyPrefixBytes);

        return iteratorAndDes(rocksIterator, valueClazz);
    }

    public <V> List<Pair<String, V>> searchMatchKeyPrefix(String keyPrefix, Class<V> valueClazz) throws IOException {
        byte[] keyPrefixBytes = super.object2Bytes(keyPrefix);

        readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
        RocksIterator rocksIterator = rocksDB.newIterator(readOptions);

        rocksIterator.seek(keyPrefixBytes);

        List<Pair<String, V>> temp = new ArrayList<>();
        while (rocksIterator.isValid()) {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();

            String key = Utils.byte2Object(keyBytes, String.class);
            V v = Utils.byte2Object(valueBytes, valueClazz);

            temp.add(new Pair<>(key, v));

            rocksIterator.next();
        }
        return temp;
    }

    private <V> List<Pair<String, V>> iteratorAndDes(RocksIterator rocksIterator, Class<V> valueClazz) throws IOException {
        List<Pair<String, V>> temp = new ArrayList<>();
        while (rocksIterator.isValid()) {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();

            String key = Utils.byte2Object(keyBytes, String.class);
            V v = Utils.byte2Object(valueBytes, valueClazz);

            temp.add(new Pair<>(key, v));

            rocksIterator.prev();
        }
        return temp;
    }

    public void deleteByKey(byte[] key) throws RocksDBException {
        rocksDB.delete(key);
    }


    public RocksDB getRocksDB() {
        return this.rocksDB;
    }

    public void close() throws Exception {

    }


    public static void main(String[] args) throws Throwable {
        RocksDBStore rocksDBStore = new RocksDBStore();

        String key = "key@1@1";
        String key2 = "key@1@2";
        Object value = "3";
        Object value2 = "2";

        byte[] keyBytes = rocksDBStore.object2Bytes(key);
        byte[] valueBytes = rocksDBStore.object2Bytes(value);

        byte[] keyBytes2 = rocksDBStore.object2Bytes(key2);
        byte[] valueBytes2 = rocksDBStore.object2Bytes(value2);

        rocksDBStore.put(keyBytes2, valueBytes2);
        rocksDBStore.put(keyBytes, valueBytes);


        byte[] bytes = rocksDBStore.get(keyBytes);
        Object result = rocksDBStore.byte2Object(bytes, Object.class);
        System.out.println(result);

        byte[] bytes2 = rocksDBStore.get(keyBytes2);
        Object result2 = rocksDBStore.byte2Object(bytes2, Object.class);
        System.out.println(result2);

        List<Pair<String, Object>> pairs = rocksDBStore.searchMatchKeyPrefix("key", Object.class);
        for (Pair<String, Object> pair : pairs) {
            System.out.println(pair);
        }

    }
}
