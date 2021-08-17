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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * sinkcache(key:String,value:String) for special scene which read operation more than write operation, it's not thread safe
 */
public class SplitCache<T> {

    private int capacity;

    /**
     * TODO: try huffman code?
     */
    private List<byte[]> values;

    private IntValueKV keys;

    private SplitCache() {

    }

    public SplitCache(int capacity) {
        this.capacity = capacity;
        values = new ArrayList<>(capacity);
        keys = new IntValueKV(capacity);
    }

    public void put(String key, String value) {
        int index = Optional.ofNullable(keys.get(key)).orElse(-1);
        if (index > -1 && index < capacity) {
            values.set(index, value.getBytes());
        } else {
            values.add(value.getBytes());
            keys.put(key, values.size() - 1);
        }
    }

    public String get(String key) {
        int index = Optional.ofNullable(keys.get(key)).orElse(-1);
        return (index > -1 && index < capacity) ? new String(values.get(index)) : null;
    }

    public String remove(String key) {
        int index = Optional.ofNullable(keys.get(key)).orElse(-1);
        String value = null;
        if (index > -1 && index < capacity) {
            value = new String(values.get(index));
            //TODO remove from keys
            values.remove(index);
        }
        return value;
    }

    public int getSize() {
        return values.size();
    }

    public void clear() {
        values = new ArrayList<>(capacity);
        keys = new IntValueKV(capacity);
    }

    public int getCapacity() {
        return capacity;
    }

}
