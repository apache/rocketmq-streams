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
package org.apache.rocketmq.streams.state.kv.rocksdb;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.state.LruState;
import org.apache.rocketmq.streams.state.kv.IKvState;
import org.rocksdb.*;

import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator.UTF8;

/**
 * kv state based rocksdb
 *
 * @author arthur.liang
 */
public class RocksdbState implements IKvState<String, String> {

    private static RocksDBOperator operator = new RocksDBOperator();

    private final LruState<String> cache = new LruState<>(100, "");

    private final static Byte SIGN = 1;

    @Override public String get(String key) {
        try {
            return getValueFromByte(operator.getInstance().get(getKeyBytes(key)));
        } catch (Exception e) {
            return null;
        }
    }

    @Override public Map<String, String> getAll(List<String> keys) {
        if (CollectionUtil.isEmpty(keys)) {
            return new HashMap<>(4);
        }
        List<byte[]> keyByteList = new ArrayList<>();
        List<String> keyStrList = new ArrayList<>();
        for (String key : keys) {
            keyByteList.add(getKeyBytes(key));
            keyStrList.add(key);
        }
        try {
            Map<String, String> resultMap = new HashMap<>(keys.size());
            Map<byte[], byte[]> map = operator.getInstance().multiGet(keyByteList);
            Iterator<Map.Entry<byte[], byte[]>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<byte[], byte[]> entry = it.next();
                String key = getValueFromByte(entry.getKey());
                String value = getValueFromByte(entry.getValue());
                resultMap.put(key, value);
            }
            return resultMap;
        } catch (RocksDBException e) {
            throw new RuntimeException("failed in getting all from rocksdb!", e);
        }
    }

    @Override public String put(String key, String value) {
        Map<String, String> map = new HashMap<String, String>(4) {{
            put(key, value);
        }};
        putAll(map);
        return null;
    }

    @Override public String putIfAbsent(String key, String value) {
        if (cache.search(key) > 0) {
            return null;
        }
        put(key, value);
        cache.add(key);
        return null;
    }

    @Override public void putAll(Map<? extends String, ? extends String> map) {
        if (map == null) {
            return;
        }
        try {
            WriteBatch writeBatch = new WriteBatch();
            Iterator<? extends Map.Entry<? extends String, ? extends String>> it = map.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<? extends String, ? extends String> entry = it.next();
                String key = entry.getKey();
                String value = entry.getValue();
                writeBatch.put(key.getBytes(UTF8), value.getBytes(UTF8));
            }

            WriteOptions writeOptions = new WriteOptions();
            writeOptions.setSync(false);
            writeOptions.setDisableWAL(true);
            operator.getInstance().write(writeOptions, writeBatch);
            writeBatch.close();
            writeOptions.close();
        } catch (Exception e) {
            throw new RuntimeException("failed in putting all into rocksdb!", e);
        }
    }

    @Override public String remove(String key) {
        try {
            operator.getInstance().delete(getKeyBytes(key));
        } catch (RocksDBException e) {
            throw new RuntimeException("failed in removing all from rocksdb! " + key, e);
        }
        return null;
    }

    @Override public void removeAll(List<String> keys) {
        for (String key : keys) {
            try {
                operator.getInstance().delete(getKeyBytes(key));
            } catch (RocksDBException e) {
                throw new RuntimeException("failed in removing all from rocksdb! " + key, e);
            }
        }
    }

    @Override public void clear() {
    }

    @Override public Iterator<String> keyIterator() {
        return null;
    }

    @Override public Iterator<Map.Entry<String, String>> entryIterator() {
        return null;
    }

    @Override public Iterator<Map.Entry<String, String>> entryIterator(String prefix) {
        return new RocksDBIterator(prefix);
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
            throw new RuntimeException("failed in getting byte[] from key! " + key, e);
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

    public static class RocksDBIterator implements Iterator<Map.Entry<String, String>> {

        protected AtomicBoolean hasInit = new AtomicBoolean(false);

        private ReadOptions readOptions = new ReadOptions();

        private RocksIterator iter;

        protected String keyPrefix;

        public RocksDBIterator(String keyPrefix) {
            readOptions.setPrefixSameAsStart(true).setTotalOrderSeek(true);
            iter = operator.getInstance().newIterator(readOptions);
            this.keyPrefix = keyPrefix;
        }

        @Override public boolean hasNext() {
            if (hasInit.compareAndSet(false, true)) {
                iter.seek(keyPrefix.getBytes());
            }
            return iter.isValid();
        }

        @Override public Map.Entry<String, String> next() {
            String key = new String(iter.key());
            if (!key.startsWith(keyPrefix)) {
                return null;
            }
            String value = getValueFromByte(iter.value());
            iter.next();
            return new Element(key, value);
        }

    }

    private static class Element implements Map.Entry<String, String> {

        private Pair<String, String> pair;

        private Element() {

        }

        public Element(String key, String value) {
            pair = Pair.of(key, value);
        }

        @Override public String getKey() {
            if (pair != null) {
                return pair.getKey();
            }
            return null;
        }

        @Override public String getValue() {
            if (pair != null) {
                return pair.getRight();
            }
            return null;
        }

        @Override public String setValue(String value) {
            if (pair != null) {
                String old = pair.getRight();
                pair.setValue(value);
                return old;
            }
            return null;
        }
    }

}
