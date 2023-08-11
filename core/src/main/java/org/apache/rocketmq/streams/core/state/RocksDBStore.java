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


import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.util.ColumnFamilyUtil;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocksDBStore extends AbstractStore implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RocksDBStore.class);

    private static final String ROCKSDB_PATH = "/tmp/rocksdb";
    private RocksDB rocksDB;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private File storeFile;

    public RocksDBStore(String path) {
        createRocksDB(path);
    }

    private void createRocksDB(String path) {
        try (final Options options = new Options().setCreateIfMissing(true)) {

            try {
                String rocksdbFilePath = String.format("%s/%s", ROCKSDB_PATH, path);

                storeFile = new File(rocksdbFilePath);

                if (storeFile.exists()) {
                    FileUtils.forceDelete(storeFile);
                }

                if (!storeFile.mkdirs()) {
                    throw new RuntimeException("before create rocksdb,mkdir path " + rocksdbFilePath + " error");
                }

                this.rocksDB = TtlDB.open(options, rocksdbFilePath, 10800, false);
                ColumnFamilyUtil.createColumnFamilies(this.rocksDB, new ColumnFamilyOptions());

                writeOptions = new WriteOptions();
                writeOptions.setSync(false);
                writeOptions.setDisableWAL(true);
            } catch (RocksDBException e) {
                throw new RuntimeException("create rocksdb error " + e.getMessage());
            } catch (IOException e) {
                throw new RuntimeException("delete rocksdb directory:" + ROCKSDB_PATH + "field.");
            }
        }
    }

    public byte[] get(String columnFamilyName, byte[] key) throws RocksDBException {
        if (key == null) {
            return null;
        }

        return rocksDB.get(ColumnFamilyUtil.getColumnFamilyHandleByName(columnFamilyName), key);
    }

    public void put(String columnFamilyName, byte[] key, byte[] value) throws RocksDBException {
        rocksDB.put(ColumnFamilyUtil.getColumnFamilyHandleByName(columnFamilyName), writeOptions, key, value);
    }

    public List<Pair<byte[], byte[]>> searchStateLessThanWatermark(String name,
                                                                   long lessThanThisTime,
                                                                   ValueMapperAction<byte[], WindowKey> deserializer) throws Throwable {
        readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);

        RocksIterator rocksIterator = rocksDB.newIterator(ColumnFamilyUtil.getColumnFamilyHandleByName(ColumnFamilyUtil.WINDOW_STATE_CF), readOptions);
        byte[] keyBytePrefix = name.getBytes(StandardCharsets.UTF_8);
        rocksIterator.seek(keyBytePrefix);

        List<Pair<byte[], byte[]>> temp = new ArrayList<>();
        while (rocksIterator.isValid()) {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();
            rocksIterator.next();

            WindowKey windowKey = deserializer.convert(keyBytes);
            if (!windowKey.getOperatorName().equals(name)) {
                continue;
            }

            if (windowKey.getWindowEnd() >= lessThanThisTime) {
                continue;
            }

            Pair<byte[], byte[]> pair = new Pair<>(keyBytes, valueBytes);
            temp.add(pair);
        }
        return temp;
    }

    public List<Pair<String, byte[]>> searchByKeyPrefix(String keyPrefix,
                                                        ValueMapperAction<String, byte[]> string2Bytes,
                                                        ValueMapperAction<byte[], String> byte2String) throws Throwable {
        readOptions = new ReadOptions();
        readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
        RocksIterator rocksIterator = rocksDB.newIterator(readOptions);

        byte[] convert = string2Bytes.convert(keyPrefix);
        rocksIterator.seek(convert);

        List<Pair<String, byte[]>> temp = new ArrayList<>();
        while (rocksIterator.isValid()) {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();

            if (skipWatermarkKey(keyBytes)) {
                continue;
            }

            String storeKey = byte2String.convert(keyBytes);
            if (storeKey.startsWith(keyPrefix)) {
                Pair<String, byte[]> pair = new Pair<>(storeKey, valueBytes);
                temp.add(pair);
            }

            rocksIterator.next();
        }

        return temp;
    }

    public void deleteByKey(String columnFamilyName, byte[] key) throws RocksDBException {
        rocksDB.delete(ColumnFamilyUtil.getColumnFamilyHandleByName(columnFamilyName), key);
    }

    public void close() throws Exception {
        this.rocksDB.close();
        if (this.storeFile != null && storeFile.exists()) {
            FileUtils.forceDelete(storeFile);
            logger.info("close RocksDB success, delete path:{}", storeFile.getPath());
        }
    }

    //todo: column family to solve this problem.
    private boolean skipWatermarkKey(byte[] target) {
        if (target == null || target.length == 0) {
            return false;
        }

        try {
            String key = new String(target, StandardCharsets.UTF_8);

            return !StringUtils.isBlank(key) && key.startsWith(Constant.WATERMARK_KEY);
        } catch (Throwable ignored) {
            return false;
        }
    }


    public static void main(String[] args) throws Throwable {
        RocksDBStore rocksDBStore = new RocksDBStore("test");

        String key = "time@1668249210000@1668249195000";
        String key2 = "time@1668249210001@1668249195001";
        Object value = "3";
        Object value2 = "2";

        byte[] keyBytes = Utils.object2Byte(key);
        byte[] valueBytes = Utils.object2Byte(value);

        byte[] keyBytes2 = Utils.object2Byte(key2);
        byte[] valueBytes2 = Utils.object2Byte(value2);

        rocksDBStore.put(ColumnFamilyUtil.getColumnFamilyByKey(keyBytes2), keyBytes2, valueBytes2);
        rocksDBStore.put(ColumnFamilyUtil.getColumnFamilyByKey(keyBytes), keyBytes, valueBytes);


        byte[] bytes = rocksDBStore.get(ColumnFamilyUtil.getColumnFamilyByKey(keyBytes), keyBytes);
        Object result = Utils.byte2Object(bytes, Object.class);
        System.out.println(result);

        byte[] bytes2 = rocksDBStore.get(ColumnFamilyUtil.getColumnFamilyByKey(keyBytes2), keyBytes2);
        Object result2 = Utils.byte2Object(bytes2, Object.class);
        System.out.println(result2);

        String keyPrefix = "time@1668249210000";


        List<Pair<String, byte[]>> pairs = rocksDBStore.searchByKeyPrefix(keyPrefix, Utils::object2Byte, data -> Utils.byte2Object(data, String.class));
        for (Pair<String, byte[]> pair : pairs) {
            assert pair.getKey().startsWith(keyPrefix);
        }

    }
}
