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
package org.apache.rocketmq.streams.window.storage.rocksdb;

import com.alibaba.fastjson.JSONArray;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.AbstractWindowStorage;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

public class RocksdbStorage<T extends WindowBaseValue> extends AbstractWindowStorage<T> {
    protected static String DB_PATH = "/tmp/rocksdb";
    protected static String UTF8 = "UTF8";
    protected static AtomicBoolean hasCreate = new AtomicBoolean(false);
    protected static RocksDB rocksDB = new RocksDBOperator().getInstance();
    protected WriteOptions writeOptions = new WriteOptions();



    @Override
    public void removeKeys(Collection<String> keys) {

        for (String key : keys) {
            try {
                rocksDB.delete(getKeyBytes(key));
            } catch (RocksDBException e) {
                throw new RuntimeException("delete error " + key);
            }
        }

    }

    @Override
    public WindowBaseValueIterator<T> loadWindowInstanceSplitData(String localStorePrefix, String queueId, String windowInstanceId, String key, Class<T> clazz) {
        String keyPrefix = MapKeyUtil.createKey(queueId, windowInstanceId, key);
        if (StringUtil.isNotEmpty(localStorePrefix)) {
            keyPrefix = localStorePrefix + keyPrefix;
        }
        return getByKeyPrefix(keyPrefix, clazz, false);
    }

    @Override public Long getMaxSplitNum(WindowInstance windowInstance, Class<T> clazz) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public void multiPut(Map<String, T> values) {
        if (values == null) {
            return;
        }
        try {
            WriteBatch writeBatch = new WriteBatch();
            Iterator<Entry<String, T>> it = values.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, T> entry = it.next();
                String key = entry.getKey();
                String value = entry.getValue().toJson();
                writeBatch.put(key.getBytes(UTF8), value.getBytes(UTF8));
            }

            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);
            rocksDB.write(writeOptions, writeBatch);
            writeBatch.close();
            writeOptions.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override
    public Map<String, T> multiGet(Class<T> clazz, List<String> keys) {
        if (keys == null || keys.size() == 0) {
            return new HashMap<>();
        }
        List<byte[]> keyByteList = new ArrayList<>();
        List<String> keyStrList = new ArrayList<>();
        for (String key : keys) {
            keyByteList.add(getKeyBytes(key));
            keyStrList.add(key);
        }
        try {
            Map<String, T> jsonables = new HashMap<>();
            //            List<byte[]>  list=  rocksDB.multiGetAsList(keyByteList);
            Map<byte[], byte[]> map = rocksDB.multiGet(keyByteList);
            int i = 0;
            Iterator<Entry<byte[], byte[]>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                String key = getValueFromByte(entry.getKey());
                String value = getValueFromByte(entry.getValue());
                T jsonable = ReflectUtil.forInstance(clazz);
                jsonable.toObject(value);
                jsonables.put(key, jsonable);
            }
            //            for(byte[] bytes:list){
            return jsonables;
        } catch (RocksDBException e) {
            throw new RuntimeException("can not get value from rocksdb ", e);
        }

    }

    @Override public void multiPutList(Map<String, List<T>> elements) {
        if (CollectionUtil.isEmpty(elements)) {
            return;
        }
        try {
            WriteBatch writeBatch = new WriteBatch();
            Iterator<Entry<String, List<T>>> it = elements.entrySet().iterator();
            while (it.hasNext()) {
                Entry<String, List<T>> entry = it.next();
                String key = entry.getKey();
                List<T> valueList = entry.getValue();
                JSONArray array = new JSONArray();
                for (T value : valueList) {
                    array.add(value.toJsonObject());
                }
                writeBatch.put(key.getBytes(UTF8), array.toJSONString().getBytes(UTF8));
            }
            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);
            rocksDB.write(writeOptions, writeBatch);
            writeBatch.close();
            writeOptions.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("put data to rocksdb error", e);
        }
    }

    @Override public Map<String, List<T>> multiGetList(Class<T> clazz, List<String> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return new HashMap<>(4);
        }
        List<byte[]> keyByteList = new ArrayList<>();
        for (String key : keys) {
            keyByteList.add(getKeyBytes(key));
        }
        try {
            Map<String, List<T>> resultMap = new HashMap<>();
            Map<byte[], byte[]> map = rocksDB.multiGet(keyByteList);
            int i = 0;
            Iterator<Entry<byte[], byte[]>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                String key = getValueFromByte(entry.getKey());
                String value = getValueFromByte(entry.getValue());
                JSONArray array = JSONArray.parseArray(value);
                List<T> valueList = new ArrayList<>();
                for (int index = 0; index < array.size(); index++) {
                    String objectString = array.getString(index);
                    T valueObject = ReflectUtil.forInstance(clazz);
                    valueObject.toObject(objectString);
                    valueList.add(valueObject);
                }
                resultMap.put(key, valueList);
            }
            return resultMap;
        } catch (RocksDBException e) {
            e.printStackTrace();
            throw new RuntimeException("can not get multi value from rocksdb! ", e);
        }
    }

    @Override
    public void clearCache(ISplit split, Class<T> clazz) {
        deleteRange(split.getQueueId(),  clazz);
    }



    @Override
    public void delete(String windowInstanceId, String queueId, Class<T> clazz) {
        //范围删除影响性能，改成了通过removekey删除
        //String plusWindowInstaceId=null;
        //  String lastWord=windowInstanceId.substring(windowInstanceId.length()-2,windowInstanceId.length());
        String firstKey = MapKeyUtil.createKey(queueId, windowInstanceId);
        deleteRange(firstKey, clazz);

    }

    protected void deleteRange(String startKey, Class<T> clazz) {
        try {
            // rocksDB.deleteRange(getKeyBytes(startKey),getKeyBytes(endKey));
            WindowBaseValueIterator<T> iterator = getByKeyPrefix(startKey, clazz, true);
            Set<String> deleteKeys = new HashSet<>();
            while (iterator.hasNext()) {
                WindowBaseValue windowBaseValue = iterator.next();
                if (windowBaseValue == null) {
                    continue;
                }
                deleteKeys.add(windowBaseValue.getMsgKey());
                if (deleteKeys.size() >= 1000) {
                    this.removeKeys(deleteKeys);
                    deleteKeys = new HashSet<>();
                }
            }
            if (deleteKeys.size() > 0) {
                this.removeKeys(deleteKeys);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected WindowBaseValueIterator<T> getByKeyPrefix(String keyPrefix, Class<? extends T> clazz, boolean needKey) {
        return new LocalIterator<T>(keyPrefix, clazz, needKey);
    }

    public static class LocalIterator<T extends WindowBaseValue> extends WindowBaseValueIterator<T> {
        protected volatile boolean hasNext = true;
        protected AtomicBoolean hasInit = new AtomicBoolean(false);
        ReadOptions readOptions = new ReadOptions();
        private RocksIterator iter;
        protected String keyPrefix;
        protected Class<? extends T> clazz;
        protected boolean needKey;

        public LocalIterator(String keyPrefix, Class<? extends T> clazz, boolean needKey) {
            readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
            iter = rocksDB.newIterator(readOptions);
            this.keyPrefix = keyPrefix;
            this.clazz = clazz;
            this.needKey = needKey;
        }

        @Override
        public boolean hasNext() {
            if (hasInit.compareAndSet(false, true)) {
                iter.seek(keyPrefix.getBytes());
            }
            return iter.isValid() && hasNext;
        }

        @Override
        public T next() {
            String key = new String(iter.key());
            if (!key.startsWith(keyPrefix)) {
                hasNext = false;
                return null;
            }
            String value = getValueFromByte(iter.value());
            T windowBaseValue = ReflectUtil.forInstance(clazz);
            windowBaseValue.toObject(value);
            if (needKey) {
                windowBaseValue.setMsgKey(key);
            }
            while (windowBaseValue.getPartitionNum() < this.partitionNum) {
                iter.next();
                windowBaseValue = next();
                if (windowBaseValue == null) {
                    hasNext = false;
                    return null;
                }
            }
            iter.next();
            return windowBaseValue;
        }

    }

    /**
     * 把key转化成byte
     *
     * @param key
     * @return
     */
    protected byte[] getKeyBytes(String key) {
        try {
            if (StringUtil.isEmpty(key)) {
                return null;
            }
            return key.getBytes(UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("get bytes error ", e);
        }
    }

    /**
     * 把byte转化成值
     *
     * @param bytes
     * @return
     */
    protected static String getValueFromByte(byte[] bytes) {
        try {
            return new String(bytes, UTF8);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        String x = "2012-01-03 00:03:09";
        System.out.println(x.substring(x.length() - 2, x.length()));
    }
}
