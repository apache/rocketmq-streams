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
package org.apache.rocketmq.streams.common.cache.compress.impl;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import org.apache.rocketmq.streams.common.cache.compress.AdditionStore;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.CacheKV;
import org.apache.rocketmq.streams.common.cache.compress.MapAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.junit.Assert;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型
 */
public class IntValueKV extends CacheKV<Integer> {

    protected AdditionStore conflicts = new AdditionStore(4);

    @Override
    public Integer get(String key) {
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        int value = byteArray.castInt(0, 4);
        if (value < MAX_INT_VALUE) {
            return value;
        }
        MapAddress mapAddress = new MapAddress(byteArray);
        ByteArray bytes = conflicts.getValue(mapAddress);
        return bytes.castInt(0, 4);
    }

    @Override
    public void put(String key, Integer value) {
        if (value < MAX_INT_VALUE) {
            super.putInner(key, value, true);
            return;
        }
        MapAddress address = conflicts.add2Store(NumberUtils.toByte(value));
        super.putInner(key, NumberUtils.toInt(address.createBytes()), true);

    }

    @Override
    public boolean contains(String key) {
        Integer value = get(key);
        if (value == null) {
            return false;
        }
        return true;

    }

    @Override
    public int calMemory() {
        return super.calMemory() + (this.conflicts.getConflictIndex() + 1) * this.conflicts.getBlockSize();
    }

    /**
     * TODO remove the key from the sinkcache and return the removed value
     *
     * @return
     */
    public IntValueKV(int capacity) {
        super(capacity);
    }

    public static void main(String[] args) throws Exception {
        IntValueKV cache = new IntValueKV(5);
        cache.put("A", 0);
        cache.put("B", 1);
        cache.put("C", 2);
        cache.put("D", 3);
        cache.put("E", 4);
        cache.put("F", 5);
        cache.put("G", 6);

        System.exit(0);

        int size = 10000000;
        int sampleSize = 1024;
        int dataSize = 3974534;
        IntValueKV compressByteMap = new IntValueKV(size);
        Map<String, Integer> dataMap = new HashMap<>(size);
        Set<Integer> whiteSet = new HashSet<>(1024);
        Map<String, Integer> sample1Map = new HashMap<>(1024);
        Map<String, Integer> sample2Map = new HashMap<>(1024);
        //init data
        Random random = new Random();
        while (true) {
            if (whiteSet.size() >= sampleSize) {
                break;
            }
            int seed = random.nextInt(dataSize);
            if (!whiteSet.contains(seed)) {
                whiteSet.add(seed);
            }
        }

        long originWriteCounter = 0;
        long compressWriteCounter = 0;
        String path = "/Users/arthur/Downloads/";
        String blackFile = "2020-11-11-14-08-32_EXPORT_CSV_16231630_392_0.csv";
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path + blackFile)))) {
            reader.readLine();
            String line = null;
            int counter = 0;
            while ((line = reader.readLine()) != null) {
                line = line.replaceAll("\"", "");
                String[] parts = line.split(",", 2);
                long begin = System.nanoTime();
                dataMap.put(parts[1].trim(), Integer.parseInt(parts[0]));
                originWriteCounter += (System.nanoTime() - begin);
                if (whiteSet.contains(counter++)) {
                    sample1Map.put(parts[1].trim(), Integer.parseInt(parts[0]));
                }
            }
        }
        for (int i = 0; i < sampleSize * 100; i++) {
            sample2Map.put(UUID.randomUUID().toString(), -1);
        }
        System.out.println("sample1 size:\t" + sample1Map.size());
        System.out.println("sample2 size:\t" + sample2Map.size());
        //System.out.println(
        //    "origin map size(computed by third party):\t" + RamUsageEstimator.humanSizeOf(dataMap) + "\tline's\t"
        //        + dataMap.size());
        //
        Iterator<Entry<String, Integer>> iterator = dataMap.entrySet().iterator();
        while (iterator.hasNext()) {
            Entry<String, Integer> entry = iterator.next();
            long begin = System.nanoTime();
            compressByteMap.put(entry.getKey(), entry.getValue());
            compressWriteCounter += (System.nanoTime() - begin);
        }
        //System.out.println(
        //    "compressed map size(computed by third party):\t" + RamUsageEstimator.humanSizeOf(compressByteMap)
        //        + "\tline's\t"
        //        + compressByteMap.size);
        System.out.println("compressed map size(computed by it's self)\t" + compressByteMap.calMemory() + " MB");
        System.out.println(
            "origin write cost:\t" + originWriteCounter / 1000 + "\tcompress write cost:\t"
                + compressWriteCounter / 1000);
        //
        long originSearchCounter = 0;
        long compressCounter = 0;
        Iterator<Entry<String, Integer>> iterator1 = sample1Map.entrySet().iterator();
        Iterator<Entry<String, Integer>> iterator2 = sample2Map.entrySet().iterator();
        while (iterator1.hasNext() && iterator2.hasNext()) {
            Entry<String, Integer> entry1 = iterator1.next();
            String key1 = entry1.getKey();
            Integer value1 = entry1.getValue();
            Entry<String, Integer> entry2 = iterator2.next();
            String key2 = entry2.getKey();
            Integer value2 = entry2.getValue();
            long begin = System.nanoTime();
            Assert.assertEquals(value1, dataMap.get(key1));
            Assert.assertNotEquals(value2, dataMap.get(key2));
            originSearchCounter += (System.nanoTime() - begin);
            begin = System.nanoTime();
            Assert.assertEquals(value1, compressByteMap.get(key1));
            Assert.assertNotEquals(value2, compressByteMap.get(key2));
            compressCounter += (System.nanoTime() - begin);
        }
        System.out.println(
            "origin search cost:\t" + originSearchCounter / 1000 + "\tcompress search cost:\t"
                + compressCounter / 1000);
    }

}
