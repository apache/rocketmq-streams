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
package org.apache.rocketmq.streams.window.storage.file;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;
import org.apache.rocketmq.streams.window.storage.AbstractWindowStorage;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

public class FileStorage<T extends WindowBaseValue> extends AbstractWindowStorage<T> {
    private static final String SPLIT_SIGN = "############";
    protected transient String filePath = "/tmp/storage/file.storage";
    protected transient Map<String, String> cache = new HashMap<>();

    @Override
    public synchronized void clearCache(ISplit channelQueue, Class<T> clazz) {
        String queueId = channelQueue.getQueueId();
        deleteByKeyPrefix(queueId);
        this.flush();
    }

    @Override
    public synchronized void delete(String windowInstanceId, String queueId, Class<T> clazz) {
        String firstKey = MapKeyUtil.createKey(queueId, windowInstanceId);
        deleteByKeyPrefix(firstKey);
        this.flush();
    }

    @Override
    public synchronized WindowBaseValueIterator<T> loadWindowInstanceSplitData(String localStorePrefix, String queueId,
                                                                               String windowInstanceId, String key, Class<T> clazz) {
        String keyPrefix = MapKeyUtil.createKey(queueId, windowInstanceId, key);
        if (StringUtil.isNotEmpty(localStorePrefix)) {
            keyPrefix = localStorePrefix + keyPrefix;
        }
        final String keyPrefixFinnal = keyPrefix;
        Map<String, String> copyCache = new HashMap<>();
        copyCache.putAll(this.cache);
        final Iterator<Entry<String, String>> iter = copyCache.entrySet().iterator();
        return new WindowBaseValueIterator<T>() {

            @Override
            public boolean hasNext() {
                return iter.hasNext();
            }

            @Override
            public T next() {
                while (iter.hasNext()) {
                    Entry<String, String> entry = iter.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (key.startsWith(keyPrefixFinnal)) {
                        T jsonable = ReflectUtil.forInstance(clazz);
                        jsonable.toObject(value);
                        return jsonable;
                    }
                }
                return null;
            }
        };
    }

    @Override public Long getMaxSplitNum(WindowInstance windowInstance, Class<T> clazz) {
        return null;
    }

    @Override
    public synchronized void multiPut(Map<String, T> map) {
        for (Entry<String, T> entry : map.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue().toJson();
            this.cache.put(key, value);
        }
        this.flush();
    }

    @Override
    public synchronized Map<String, T> multiGet(Class<T> clazz, List<String> keys) {
        Map<String, T> result = new HashMap<>();
        for (String key : keys) {
            String value = this.cache.get(key);
            if (StringUtil.isNotEmpty(value)) {
                T jsonable = ReflectUtil.forInstance(clazz);
                jsonable.toObject(value);
                result.put(key, jsonable);
            }
        }
        return result;
    }

    @Override
    public synchronized void removeKeys(Collection<String> keys) {
        for (String key : keys) {
            this.cache.remove(key);
        }
        this.flush();
    }

    protected synchronized void deleteByKeyPrefix(String keyPrefix) {
        Map<String, String> copyCache = new HashMap<>();
        copyCache.putAll(this.cache);
        Iterator<Entry<String, String>> it = copyCache.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            if (key.startsWith(keyPrefix)) {
                cache.remove(key);
            }
        }
    }

    private synchronized void flush() {
        List<String> buffer = new ArrayList<>();
        Map<String, String> copyCache = new HashMap<>();
        copyCache.putAll(this.cache);

        Iterator<Entry<String, String>> it = copyCache.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, String> entry = it.next();
            String key = entry.getKey();
            String value = entry.getValue();
            String line = MapKeyUtil.createKeyBySign(SPLIT_SIGN, key, value);
            buffer.add(line);
        }
        FileUtil.write(filePath, buffer, false);
    }

    private void load() {
        Map<String, String> cache = new HashMap<>();
        List<String> buffer = FileUtil.loadFileLine(filePath);
        for (String line : buffer) {
            String[] values = line.split(SPLIT_SIGN);
            cache.put(values[0], values[1]);
        }
        this.cache = cache;
    }

    //_order_by_split_num_1;1;namespace;name_window_10001;2021-07-13 15:07:40;2021-07-13 02:35:10;2021-07-13 02:35:15

    public static void main(String[] args) {
        FileStorage fileStorage = new FileStorage();
        fileStorage.load();
        fileStorage.deleteByKeyPrefix("_order_by_split_num_1;1;namespace;name_window_10001;2021-07-13 15:07:40;2021-07-13 02:35:10;2021-07-13 02:35:15");
        fileStorage.deleteByKeyPrefix("1;1;namespace;name_window_10001;2021-07-13 15:07:40;2021-07-13 02:35:10;2021-07-13 02:35:15");
        fileStorage.deleteByKeyPrefix("1;1;namespace;name_window_10001;2021-07-13 15:07:40;2021-07-13 02:35:05;2021-07-13 02:35:10");
        fileStorage.deleteByKeyPrefix(" _order_by_split_num_1;1;namespace;name_window_10001;2021-07-13 15:07:40;2021-07-13 02:35:05;2021-07-13 02:35:10");
        fileStorage.flush();
        fileStorage.load();
        WindowBaseValueIterator<WindowValue> fileIterator = fileStorage.loadWindowInstanceSplitData("_order_by_split_num_", "1", "1;namespace;name_window_10001",
            null, WindowValue.class);
        int sum = 0;
        while (fileIterator.hasNext()) {
            WindowValue windowValue = fileIterator.next();
            if (windowValue == null) {
                break;
            }
            sum++;
        }

        fileIterator = fileStorage.loadWindowInstanceSplitData(null, "1", "1;namespace;name_window_10001",
            null, WindowValue.class);
        while (fileIterator.hasNext()) {
            WindowValue windowValue = fileIterator.next();
            if (windowValue == null) {
                break;
            }
            sum++;
        }
        System.out.println(sum);

    }
}
