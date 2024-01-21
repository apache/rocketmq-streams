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

import java.util.List;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.ByteStore;
import org.apache.rocketmq.streams.common.cache.compress.KVAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class LongListKV extends AbstractListKV<Long> {

    protected ByteStore values = new ByteStore(12);

    public LongListKV(int capacity) {
        super(capacity);
    }

    private static int calHashCode(String key) {
        int hashCode = key.hashCode();
        int value = hashCode ^ (hashCode >>> 16);
        value = String.valueOf(value).hashCode();
        return value;
    }

    @Override protected byte[] convertByte(Long value) {
        return NumberUtils.toByte(value);
    }

    @Override protected int getElementSize() {
        return 8;
    }

    @Override protected ByteStore getValues() {
        return values;
    }

    /**
     * 获取最后一个元素
     *
     * @param key
     * @param values
     * @return
     */
    @Override
    protected ByteArray getLastElement(String key, List<Long> values, ListValueAddress addresses) {
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        if (addresses != null) {
            addresses.setHeader(byteArray);
        }
        KVAddress nextAddress = new KVAddress(byteArray);
        ByteArray nextAddressAndValue = null;
        while (!nextAddress.isEmpty()) {
            if (addresses != null) {
                addresses.addAddress(nextAddress);
            }
            nextAddressAndValue = this.values.getValue(nextAddress);
            long value = nextAddressAndValue.subByteArray(0, 8).castLong(0, 8);
            ByteArray address = nextAddressAndValue.subByteArray(8, 5);
            nextAddress = new KVAddress(address);
            values.add(value);
        }
        return nextAddressAndValue;
    }
}
