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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;

public class SelfAdaptionRocksdbStorage<T extends WindowBaseValue> extends RocksdbStorage<T> {

    protected boolean isCacheModel = true;//刚启动，默认是cache模式，当数据量比较大时，会降级成rocksdb模式，可以减少序列化和反序列化的消耗
    protected int cacheMaxSize = 200000;
    protected TreeMap<String, T> cache = new TreeMap(new Comparator<String>() {

        @Override public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    });
    protected TreeMap<String, List<T>> cache2ValueList = new TreeMap(new Comparator<String>() {

        @Override public int compare(String o1, String o2) {
            return o1.compareTo(o2);
        }
    });

    public static void main(String[] args) {
        TreeMap<String, Integer> cache = new TreeMap(new Comparator<String>() {

            @Override public int compare(String o1, String o2) {
                return o1.compareTo(o2);
            }
        });
        cache.put("1", 1);
        cache.put("9", 2);
        cache.put("5", 5);
        cache.put("24", 4);
        cache.put("23", 3);
        cache.put("6", 6);
        Map.Entry<String, Integer> entry = cache.ceilingEntry("2");

        System.out.println(entry.getKey());
        Iterator iterator = cache.keySet().iterator();

        while (iterator.hasNext()) {
            String key = (String) iterator.next();
            System.out.println(key);
        }

    }

    @Override
    protected synchronized void deleteRange(String startKey, Class<T> clazz) {
        if (!isCacheModel) {
            super.deleteRange(startKey, clazz);
            return;
        }
        Map<String, ?> prefixMap = this.getPrefixSubMap(startKey, this.cache);
        if (prefixMap != null) {
            for (String key : prefixMap.keySet()) {
                cache.remove(key);
            }
        }

        Map<String, ?> prefixMapForList = this.getPrefixSubMap(startKey, this.cache2ValueList);
        if (prefixMap != null) {
            for (String key : prefixMapForList.keySet()) {
                this.cache2ValueList.remove(key);
            }
        }

    }

    @Override
    public synchronized WindowStorage.WindowBaseValueIterator<T> getByKeyPrefix(String keyPrefix, Class<? extends T> clazz, boolean needKey) {
        if (!isCacheModel) {
            return new LocalIterator<T>(keyPrefix, clazz, needKey);
        }

        Map<String, T> prefixMap = this.getPrefixSubMap(keyPrefix, this.cache);
        if (prefixMap == null) {
            return null;
        }
        Iterator<Map.Entry<String, T>> it = prefixMap.entrySet().iterator();
        return new WindowStorage.WindowBaseValueIterator<T>() {
            protected boolean hasNext = true;

            @Override public boolean hasNext() {
                return it.hasNext() && hasNext;
            }

            @Override public T next() {
                Map.Entry<String, T> entry = it.next();
                String key = entry.getKey();
                T windowBaseValue = entry.getValue();
                if (needKey) {
                    windowBaseValue.setMsgKey(key);
                }
                while (windowBaseValue.getPartitionNum() < this.partitionNum) {
                    entry = it.next();
                    key = entry.getKey();
                    windowBaseValue = entry.getValue();
                    if (windowBaseValue == null) {
                        hasNext = false;
                        return null;
                    }
                }
                if (windowBaseValue.getPartitionNum() < this.partitionNum) {
                    entry = it.next();
                }
                windowBaseValue = entry.getValue();
                return windowBaseValue;
            }
        };
    }

    @Override public synchronized void multiPut(Map<String, T> map) {
        if (!isCacheModel) {
            super.multiPut(map);
            return;
        }

        this.cache.putAll(map);
        if (getCacheSize() > this.cacheMaxSize) {
            this.isCacheModel = false;
            synCache2RocksDB();
        }

    }

    @Override synchronized public void multiPutList(Map<String, List<T>> elements) {
        if (!isCacheModel) {
            super.multiPutList(elements);
            return;
        }

        this.cache2ValueList.putAll(elements);
        if (getCacheSize() > this.cacheMaxSize) {
            this.isCacheModel = false;
            synCache2RocksDB();
        }
    }

    @Override public synchronized Map<String, T> multiGet(Class<T> clazz, List<String> keys) {
        if (!isCacheModel) {
            return super.multiGet(clazz, keys);
        }
        if (keys == null) {
            return new HashMap<>();
        }
        Map<String, T> result = new HashMap<>();
        for (String key : keys) {
            T value = this.cache.get(key);
            if (value != null) {
                result.put(key, value);
            }

        }
        return result;
    }

    @Override public synchronized Map<String, List<T>> multiGetList(Class<T> clazz, List<String> keys) {
        if (!isCacheModel) {
            return super.multiGetList(clazz, keys);
        }
        if (keys == null) {
            return new HashMap<>();
        }
        Map<String, List<T>> result = new HashMap<>();
        for (String key : keys) {
            List<T> value = this.cache2ValueList.get(key);
            if (value != null) {
                result.put(key, value);
            }
        }
        return result;
    }

    @Override public void removeKeys(Collection<String> keys) {
        if (!isCacheModel) {
            super.removeKeys(keys);
            return;
        }

        if (keys == null) {
            return;
        }
        for (String key : keys) {
            this.cache2ValueList.remove(key);
            this.cache.remove(key);
        }

    }

    private int getCacheSize() {
        return this.cache.size() + this.cache2ValueList.size();
    }

    private void synCache2RocksDB() {
        if (this.cache != null) {
            super.multiPut(this.cache);
            this.cache.clear();
        }
        if (this.cache2ValueList != null) {
            super.multiPutList(this.cache2ValueList);
            this.cache2ValueList.clear();
        }

    }

    protected <O> Map<String, O> getPrefixSubMap(String prefix, TreeMap<String, O> treeMap) {
        Iterator<String> iterator = treeMap.keySet().iterator();
        Map<String, O> result = new HashMap<>();
        boolean isMatchPrefix = false;
        while (iterator.hasNext()) {
            String key = iterator.next();
            if (key.startsWith(prefix)) {
                isMatchPrefix = true;
                result.put(key, treeMap.get(key));
            } else {
                if (isMatchPrefix) {
                    break;
                }
            }
        }
        return result;
    }

}
