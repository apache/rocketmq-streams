package org.apache.rocketmq.streams.window.storage.rocksdb;
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

import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.apache.rocketmq.streams.window.storage.AbstractStorage;
import org.apache.rocketmq.streams.window.storage.DataType;
import org.apache.rocketmq.streams.window.storage.IteratorWrap;
import org.apache.rocketmq.streams.window.storage.RocksdbIterator;
import org.apache.rocketmq.streams.window.storage.WindowJoinType;
import org.apache.rocketmq.streams.window.storage.WindowType;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocksdbStorage extends AbstractStorage {
    private RocksDB rocksDB;
    private WriteOptions writeOptions;

    public RocksdbStorage() {
        rocksDB = new RocksDBOperator().getInstance();
        writeOptions = new WriteOptions();
        writeOptions.setSync(false);
        writeOptions.setDisableWAL(true);
    }


    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {
        if (windowInstance == null) {
            return;
        }

        //唯一键
        String windowInstanceKey = windowInstance.getWindowInstanceKey();

        String key = super.merge(DataType.WINDOW_INSTANCE.getValue(), shuffleId, windowNamespace, windowConfigureName, windowInstanceKey);

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = SerializeUtil.serialize(windowInstance);

        try {
            WriteBatch batch = new WriteBatch();
            batch.put(keyBytes, valueBytes);

            rocksDB.write(writeOptions, batch);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public <T> RocksdbIterator<T> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        String keyPrefix = super.merge(DataType.WINDOW_INSTANCE.getValue(), shuffleId, windowNamespace, windowConfigureName);

        return new RocksdbIterator<>(keyPrefix, rocksDB);
    }

    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        if (windowInstanceKey == null) {
            return;
        }

        String key = super.merge(DataType.WINDOW_INSTANCE.getValue(), shuffleId, windowNamespace, windowConfigureName, windowInstanceKey);

        try {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(bytes);
        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }
    }

    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType, List<WindowBaseValue> windowBaseValue) {
        if (windowBaseValue == null || windowBaseValue.size() == 0) {
            return;
        }


        for (WindowBaseValue baseValue : windowBaseValue) {
            doPut(baseValue, shuffleId, windowInstanceId, windowType, joinType);
        }
    }

    public void putWindowBaseValueIterator(String shuffleId, String windowInstanceId,
                                           WindowType windowType, WindowJoinType joinType,
                                           RocksdbIterator<? extends WindowBaseValue> windowBaseValueIterator) {
        if (windowBaseValueIterator == null) {
            return;
        }

        while (windowBaseValueIterator.hasNext()) {
            IteratorWrap<? extends WindowBaseValue> next = windowBaseValueIterator.next();
            WindowBaseValue data = next.getData();

            doPut(data, shuffleId, windowInstanceId, windowType, joinType);
        }
    }

    private void doPut(WindowBaseValue baseValue, String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        String key = createKey(shuffleId, windowInstanceId, windowType, joinType, baseValue);

        try {
            byte[] valueBytes;

            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            valueBytes = SerializeUtil.serialize(baseValue);
            rocksDB.put(keyBytes, valueBytes);
        } catch (Throwable t) {
            throw new RuntimeException("put data to rocksdb error", t);
        }

    }


    @Override
    public <T> RocksdbIterator<T> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

        String keyPrefix = createKey(shuffleId, windowInstanceId, windowType, joinType, null);

        return new RocksdbIterator<>(keyPrefix, rocksDB);
    }

    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        try {
            String keyPrefix = createKey(shuffleId, windowInstanceId, windowType, joinType, null);

            //查询msgKey
            RocksdbIterator<WindowBaseValue> rocksdbIterator = new RocksdbIterator<>(keyPrefix, rocksDB);

            ArrayList<String> msgKeys = new ArrayList<>();
            while (rocksdbIterator.hasNext()) {
                IteratorWrap<WindowBaseValue> baseValue = rocksdbIterator.next();
                WindowBaseValue data = baseValue.getData();
                if (data == null) {
                    continue;
                }

                msgKeys.add(data.getMsgKey());
            }

            //组合成真正的key后，在挨个删除
            for (String msgKey : msgKeys) {
                String key = super.merge(keyPrefix, msgKey);
                byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
                rocksDB.delete(bytes);
            }

        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }
    }

    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType, String msgKey) {
        String key;
        if (joinType != null) {
            key = super.merge(shuffleId, windowInstanceId, windowType.name(), joinType.name(), msgKey);
        } else {
            key = super.merge(shuffleId, windowInstanceId, windowType.name(), msgKey);
        }

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(keyBytes);
        } catch (Throwable t) {
            throw new RuntimeException("delete data to rocksdb error", t);
        }
    }

    private String createKey(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType, WindowBaseValue baseValue) {
        String result;
        switch (windowType) {
            case SESSION_WINDOW:
            case NORMAL_WINDOW: {
                result = super.merge(DataType.WINDOW_BASE_VALUE.getValue(), shuffleId, windowInstanceId, windowType.name());
                if (baseValue != null) {
                    result = super.merge(result, baseValue.getMsgKey());
                }

                break;
            }
            case JOIN_WINDOW: {
                result = super.merge(DataType.WINDOW_BASE_VALUE.getValue(), shuffleId, windowInstanceId, windowType.name(), joinType.name());

                if (baseValue != null) {
                    JoinState joinState = (JoinState) baseValue;
                    result = super.merge(result, joinState.getMessageId());
                }

                break;
            }
            default:
                throw new RuntimeException("windowType " + windowType + "illegal.");
        }

        return result;
    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = super.merge(DataType.MAX_OFFSET.getValue(), shuffleId, windowConfigureName, oriQueueId);

        try {
            byte[] bytes = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            if (bytes == null) {
                return null;
            }

            String temp = new String(bytes, StandardCharsets.UTF_8);

            List<String> split = super.split(temp);

            return split.get(1);
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        String key = super.merge(DataType.MAX_OFFSET.getValue(), shuffleId, windowConfigureName, oriQueueId);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            String mergeOffset = super.merge(getCurrentTimestamp(), offset);
            byte[] offsetBytes = mergeOffset.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, offsetBytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = super.merge(DataType.MAX_OFFSET.getValue(), shuffleId, windowConfigureName, oriQueueId);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(keyBytes);
        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }
    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        String key = super.merge(DataType.MAX_PARTITION_NUM.getValue(), shuffleId, windowInstanceKey);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            String mergeMaxPartitionNum = super.merge(getCurrentTimestamp(), String.valueOf(maxPartitionNum));

            byte[] bytes = mergeMaxPartitionNum.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, bytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = super.merge(DataType.MAX_PARTITION_NUM.getValue(), shuffleId, windowInstanceKey);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = rocksDB.get(keyBytes);
            if (bytes == null || bytes.length == 0) {
                return -1L;
            }

            String temp = new String(bytes, StandardCharsets.UTF_8);
            List<String> list = super.split(temp);

            return Long.parseLong(list.get(1));
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = super.merge(DataType.MAX_PARTITION_NUM.getValue(), shuffleId, windowInstanceKey);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(keyBytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    public void delete(String key) {
        if (key == null) {
            return;
        }

        try {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(bytes);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public byte[] get(String key) {
        if (key == null) {
            return null;
        }

        try {
            byte[] bytes = key.getBytes(StandardCharsets.UTF_8);
            return rocksDB.get(bytes);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public void put(String key, byte[] value) {
        if (key == null) {
            return;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        try {
            rocksDB.put(keyBytes, value);
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }


    @Override
    public void clearCache(String queueId) {
        String keyPrefix;
        //删除windowInstance缓存
        keyPrefix = super.merge(DataType.WINDOW_INSTANCE.getValue(), queueId);
        deleteByKeyPrefix(keyPrefix);

        keyPrefix = super.merge(DataType.WINDOW_BASE_VALUE.getValue(), queueId);
        deleteByKeyPrefix(keyPrefix);

        keyPrefix = super.merge(DataType.MAX_PARTITION_NUM.getValue(), queueId);
        deleteByKeyPrefix(keyPrefix);

        keyPrefix = super.merge(DataType.MAX_OFFSET.getValue(), queueId);
        deleteByKeyPrefix(keyPrefix);
    }

    private void deleteByKeyPrefix(String keyPrefix) {
        RocksdbIterator<Object> data = new RocksdbIterator<>(keyPrefix, rocksDB);

        while (data.hasNext()) {
            IteratorWrap<Object> iteratorWrap = data.next();
            String key = iteratorWrap.getKey();
            try {
                rocksDB.delete(writeOptions, key.getBytes(StandardCharsets.UTF_8));
            } catch (Throwable t) {
                throw new RuntimeException();
            }
        }
    }

    public <T> RocksdbIterator<T> getData(String queueId, DataType type) {
        String keyPrefix;
        switch (type) {
            case WINDOW_INSTANCE:
                keyPrefix = super.merge(DataType.WINDOW_INSTANCE.getValue(), queueId);
                break;
            case WINDOW_BASE_VALUE:
                keyPrefix = super.merge(DataType.WINDOW_BASE_VALUE.getValue(), queueId);
                break;
            case MAX_OFFSET:
                keyPrefix = super.merge(DataType.MAX_OFFSET.getValue(), queueId);
                break;
            case MAX_PARTITION_NUM:
                keyPrefix = super.merge(DataType.MAX_PARTITION_NUM.getValue(), queueId);
                break;
            default:
                throw new RuntimeException();
        }

        return new RocksdbIterator<>(keyPrefix, rocksDB);
    }


}
