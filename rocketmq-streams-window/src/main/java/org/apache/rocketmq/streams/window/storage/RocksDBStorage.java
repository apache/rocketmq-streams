package org.apache.rocketmq.streams.window.storage;
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
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class RocksDBStorage extends AbstractStorage {
    private static RocksDB rocksDB = null;
    private static WriteOptions writeOptions = null;

    @Override
    public void start() {
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

        WriteBatch batch = new WriteBatch();

        String windowInstanceKey = windowInstance.getWindowInstanceKey();
        byte[] keyBytes = windowInstanceKey.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = SerializeUtil.serialize(windowInstance);

        try {
            //1
            batch.put(keyBytes, valueBytes);
            rocksDB.write(writeOptions, batch);

            //2
            String key = super.buildKey(windowNamespace, windowConfigureName, shuffleId);
            appendValue(key, windowInstanceKey);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        String key = super.buildKey(windowNamespace, windowConfigureName, shuffleId);
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);


        try {
            byte[] bytes = rocksDB.get(keyBytes);
            if (bytes == null || bytes.length == 0) {
                return null;
            }

            String multiValue = new String(bytes, StandardCharsets.UTF_8);
            List<String> split = super.split(multiValue);

            List<WindowInstance> result = new ArrayList<>();
            for (String windowInstanceKey : split) {
                byte[] tempKey = windowInstanceKey.getBytes(StandardCharsets.UTF_8);
                byte[] targetValue = rocksDB.get(tempKey);
                WindowInstance windowInstance = SerializeUtil.deserialize(targetValue);
                result.add(windowInstance);
            }

            return result;
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }
    }

    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        if (windowInstanceKey == null) {
            return;
        }

        //只是从二级索引中删除
        try {
            byte[] bytes = windowInstanceKey.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(bytes);
        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }
    }

    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType,
                                   WindowJoinType joinType, List<WindowBaseValue> windowBaseValue) {
        if (windowBaseValue == null || windowBaseValue.size() == 0) {
            return;
        }

        try {
            for (WindowBaseValue baseValue : windowBaseValue) {
                String firstKey = null;
                String secondKey = null;
                byte[] valueBytes;

                switch (windowType) {
                    case NORMAL_WINDOW:
                    case SESSION_WINDOW:
                        firstKey = super.buildKey(windowInstanceId, windowType.name());

                        WindowValue windowValue = (WindowValue) baseValue;
                        secondKey = windowValue.getMsgKey();
                        break;
                    case JOIN_WINDOW:
                        firstKey = super.buildKey(windowInstanceId, windowType.name(), joinType.name());

                        JoinState joinState = (JoinState) baseValue;
                        secondKey = joinState.getMessageId();
                        break;
                }
                appendValue(firstKey, secondKey);

                byte[] keyBytes = secondKey.getBytes(StandardCharsets.UTF_8);
                valueBytes = SerializeUtil.serialize(baseValue);
                rocksDB.put(keyBytes, valueBytes);
            }
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }


    }

    @Override
    public List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId,
                                                    WindowType windowType, WindowJoinType joinType) {

        try {
            String key = null;

            switch (windowType) {
                case NORMAL_WINDOW:
                case SESSION_WINDOW:
                    key = buildKey(windowInstanceId, windowType.name());
                    break;
                case JOIN_WINDOW:
                    key = buildKey(windowInstanceId, windowType.name(), joinType.name());
                    break;
            }


            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            byte[] multiString = rocksDB.get(keyBytes);
            if (multiString == null || multiString.length == 0) {
                return null;
            }

            List<String> split = split(new String(multiString, StandardCharsets.UTF_8));
            ArrayList<WindowBaseValue> result = new ArrayList<>();
            for (String msgKey : split) {
                byte[] bytes = rocksDB.get(msgKey.getBytes(StandardCharsets.UTF_8));
                WindowBaseValue value = SerializeUtil.deserialize(bytes);
                result.add(value);
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }
    }

    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        try {
            String key = null;

            switch (windowType) {
                case NORMAL_WINDOW:
                case SESSION_WINDOW:
                    key = buildKey(windowInstanceId, windowType.name());
                    break;
                case JOIN_WINDOW:
                    key = buildKey(windowInstanceId, windowType.name(), joinType.name());
                    break;
            }
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

            //1 get
            byte[] bytes = rocksDB.get(keyBytes);
            if (bytes == null) {
                return;
            }

            //2 delete
            rocksDB.delete(keyBytes);

            //3 删除二级索引
            String multiString = new String(bytes, StandardCharsets.UTF_8);
            List<String> split = super.split(multiString);
            for (String secondKey : split) {
                rocksDB.delete(secondKey.getBytes(StandardCharsets.UTF_8));
            }

        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }

    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);

        try {
            byte[] bytes = rocksDB.get(key.getBytes(StandardCharsets.UTF_8));
            return new String(bytes, StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }

    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] offsetBytes = offset.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, offsetBytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        String key = buildKey(windowConfigureName, shuffleId, oriQueueId);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(keyBytes);
        } catch (Exception e) {
            throw new RuntimeException("delete data to rocksdb error", e);
        }
    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        String key = buildKey(windowInstanceKey, shuffleId);
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = String.valueOf(maxPartitionNum).getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, bytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = buildKey(windowInstanceKey, shuffleId);

        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] bytes = rocksDB.get(keyBytes);

            String temp = new String(bytes, StandardCharsets.UTF_8);
            return Long.parseLong(temp);
        } catch (Exception e) {
            throw new RuntimeException("get data to rocksdb error", e);
        }
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        String key = buildKey(windowInstanceKey, shuffleId);
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            rocksDB.delete(keyBytes);
        } catch (Exception e) {
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }



    private void appendValue(String key, String appendValue) throws RocksDBException {
        if (key == null || appendValue == null) {
            return;
        }

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);

        byte[] values = rocksDB.get(keyBytes);
        if (values == null || values.length == 0) {
            byte[] valueBytes = appendValue.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, valueBytes);
        } else {
            String unionValues = new String(values, StandardCharsets.UTF_8);
            List<String> split = super.split(unionValues);
            if (!split.contains(appendValue)) {
                split.add(appendValue);
                String newValue = buildKey(split.toArray(new String[0]));
                byte[] newValueBytes = newValue.getBytes(StandardCharsets.UTF_8);
                rocksDB.put(keyBytes, newValueBytes);
            }
        }
    }

}
