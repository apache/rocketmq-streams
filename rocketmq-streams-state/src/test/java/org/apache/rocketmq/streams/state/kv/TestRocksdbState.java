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
package org.apache.rocketmq.streams.state.kv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.streams.state.kv.rocksdb.RocksdbState;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author arthur.liang
 */
public class TestRocksdbState {

    private RocksdbState rocksdbState = new RocksdbState();

    private String key = "window_id";

    private String value = "window_value";

    private int sampleSize = 8;

    @Before
    public void testAddAll() {
        rocksdbState.put(key, value);
        Map<String, String> stateMap = new HashMap<>(8);
        for (int i = 0; i < sampleSize; i++) {
            stateMap.put(key + "_" + i, value + "_" + i);
        }
        rocksdbState.putAll(stateMap);
    }

    @Test
    public void testGetAll() {
        String singleValue = rocksdbState.get(key);
        Assert.assertEquals(value, singleValue);
        List<String> keys = new ArrayList<String>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            keys.add(key + "_" + i);
        }
        Map<String, String> valueMap = rocksdbState.getAll(keys);
        Assert.assertEquals(8, valueMap.size());
        singleValue = rocksdbState.get("any_key");
        Assert.assertEquals(null, singleValue);
    }

    @Test
    public void testIterator() {
        Iterator<Map.Entry<String, String>> iterator = rocksdbState.entryIterator(key);
        Map<String, String> valueMap = new HashMap<>(sampleSize);
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            if (entry == null) {
                break;
            }
            valueMap.put(entry.getKey(), entry.getValue());
        }
        Assert.assertEquals(9, valueMap.size());
    }

    @Test
    public void testDelete() {
        rocksdbState.remove(key);
        String singleValue = rocksdbState.get(key);
        Assert.assertEquals(null, singleValue);
        List<String> keys = new ArrayList<>(sampleSize);
        for (int i = 0; i < sampleSize; i++) {
            keys.add(key + "_" + i);
        }
        rocksdbState.removeAll(keys);
        Map<String, String> valueMap = rocksdbState.getAll(keys);
        Assert.assertEquals(0, valueMap.size());
    }

    @Test
    public void testOverWrite() {
        String replaceValue = value + "_new";
        rocksdbState.put(key, replaceValue);
        String replaceResult = rocksdbState.get(key);
        Assert.assertEquals(replaceResult, replaceValue);
    }

    @Test
    public void testNotOverWrite() {

    }

}
