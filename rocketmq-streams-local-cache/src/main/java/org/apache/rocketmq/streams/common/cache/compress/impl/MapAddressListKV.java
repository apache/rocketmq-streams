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
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.ByteStore;
import org.apache.rocketmq.streams.common.cache.compress.KVAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class MapAddressListKV extends AbstractListKV<KVAddress> {

    protected ByteStore values = new ByteStore(10);

    public MapAddressListKV(int capacity) {
        super(capacity);
    }

    public static void main(String[] args) {
        int rowSize = 50000000;
        MapAddressListKV intListKV = new MapAddressListKV(rowSize);
        List<Long> values = null;

        for (int i = 0; i < rowSize; i++) {
            intListKV.addLongValue("chris" + i, Long.valueOf(i));
            List<Long> rowIds = intListKV.getLongValue("chris" + i);
            if (i != rowIds.get(0).intValue()) {
                throw new RuntimeException("expect " + i + "  " + rowIds.get(0));
            }
        }
        System.out.println("success");
    }

    @Override protected byte[] convertByte(KVAddress value) {
        return value.createBytes();
    }

    @Override protected int getElementSize() {
        return 5;
    }

    @Override protected ByteStore getValues() {
        return values;
    }

    @Override
    protected void add(String key, KVAddress mapAddress, ByteArray last) {
        if (key == null || mapAddress == null) {
            return;
        }
        /**
         * 当前元素是第一个元素，插入到父类的conflict字段
         */
        if (last == null) {
            super.putInner(key, new ByteArray(mapAddress.createBytesIngoreFirstBit()), true);
            return;
        }
        /**
         * 原来有一个元素，在父类的conflict，需要把这个值移到本类的value字段
         */
        if (NumberUtils.isFirstBitZero(last) && last.getSize() == 5) {
            byte fisrtByte = last.getByte(4);
            int value = (fisrtByte | (1 << 7));//把第一位变成1
            last.setByte(4, (byte) (value & 0xff));
            byte[] element = createElement(last);
            KVAddress address = this.values.add2Store(element);
            byte[] addressByte = address.createBytes();
            super.putInner(key, new ByteArray(addressByte), true);
            add(key, mapAddress);
            return;
        }
        byte[] element = createElement(mapAddress.createBytes());
        KVAddress address = this.values.add2Store(element);
        byte[] nextAddress = address.createBytes();
        for (int i = 0; i < nextAddress.length; i++) {
            last.setByte(i + getElementSize(), nextAddress[i]);
        }
    }

    public List<Long> getLongValue(String key) {
        List<KVAddress> mapAddresses = get(key);
        if (mapAddresses == null) {
            return null;
        }
        List<Long> values = new ArrayList<>();
        for (KVAddress address : mapAddresses) {
            long value = address.convertLongValue();
            values.add(value);
        }
        return values;
    }

    public void addLongValue(String key, Long value) {
        add(key, KVAddress.createMapAddressFromLongValue(value));
    }

    /**
     * 创建一个元素，包含两部分：int值，int
     *
     * @param value
     * @return
     */
    private byte[] createElement(ByteArray value) {
        return createElement(value.getByteArray());
    }

    /**
     * 获取最后一个元素
     *
     * @param key
     * @param values
     * @return
     */
    @Override
    protected ByteArray getLastElement(String key, List<KVAddress> values, ListValueAddress addresses) {
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        if (addresses != null) {
            addresses.setHeader(byteArray);
        }
        if (NumberUtils.isFirstBitZero(byteArray)) {
            byte[] bytes = byteArray.getByteArray();
            values.add(KVAddress.createAddressFromBytes(bytes));
            return byteArray;
        }
        KVAddress nextAddress = new KVAddress(byteArray);
        ByteArray nextAddressAndValue = null;
        while (!nextAddress.isEmpty()) {
            if (addresses != null) {
                addresses.addAddress(nextAddress);
            }
            nextAddressAndValue = this.values.getValue(nextAddress);
            ByteArray address = nextAddressAndValue.subByteArray(5, 5);
            nextAddress = new KVAddress(address);
            values.add(new KVAddress(nextAddressAndValue.subByteArray(0, 5)));
        }
        return nextAddressAndValue;
    }
}
