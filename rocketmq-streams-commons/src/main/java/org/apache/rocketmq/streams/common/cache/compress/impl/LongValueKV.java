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

import java.nio.ByteBuffer;
import org.apache.rocketmq.streams.common.cache.compress.ByteArrayValueKV;
import org.apache.rocketmq.streams.common.cache.compress.CacheKV;

public class LongValueKV extends CacheKV<Long> {

    private final ByteArrayValueKV byteArrayValueKV;

    public LongValueKV(int capacity) {
        super(capacity, 8);
        byteArrayValueKV = new ByteArrayValueKV(capacity, true);
    }

    @Override
    public void put(String key, Long value) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(0, value);
        byte[] bytes = buffer.array();
        byteArrayValueKV.put(key, bytes);
    }

    @Override
    public boolean contains(String key) {
        return byteArrayValueKV.contains(key);
    }

    @Override
    public int getSize() {
        return byteArrayValueKV.getSize();
    }

    @Override
    public int calMemory() {
        return byteArrayValueKV.calMemory();
    }

    @Override
    public Long get(String key) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        byte[] bytes = byteArrayValueKV.get(key);
        if (bytes == null) {
            return null;
        }
        buffer.put(bytes, 0, 8);
        buffer.flip();
        return buffer.getLong();
    }

}
