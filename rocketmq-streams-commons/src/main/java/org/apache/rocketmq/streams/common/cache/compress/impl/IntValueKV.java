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

import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.ByteStore;
import org.apache.rocketmq.streams.common.cache.compress.CacheKV;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型
 */
public class IntValueKV extends CacheKV<Integer> {

    protected ByteStore conflicts = new ByteStore(4);

    @Override
    public Integer get(String key) {
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        int value = byteArray.castInt(0, 4);
        return value;
    }

    @Override
    public void put(String key, Integer value) {
        byte[] bytes = NumberUtils.toByte(value, 5);
        bytes[4] = (byte) 0;
        super.putInner(key, new ByteArray(bytes), true);
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
        super(capacity, true);
    }

}
