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
package org.apache.rocketmq.streams.common.cache.compress;

import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class KVElement {

    protected ByteArray nextAddressByte;

    protected CacheKV.MapAddress nextAddress;

    protected ByteArray keyHashCode;

    /**
     * value的值
     */
    protected ByteArray value;

    /**
     * 是否没有value
     */
    protected boolean noValue = false;

    /**
     * 元素个数
     */
    protected int elementSize = 24;

    public KVElement(ByteArray byteArray) {
        this.nextAddressByte = byteArray.subByteArray(0, 4);
        nextAddress = new CacheKV.MapAddress(this.nextAddressByte);
        this.keyHashCode = byteArray.subByteArray(4, 16);
        if (!noValue) {
            value = byteArray.subByteArray(20, 4);
        }
    }

    public static byte[] createByteArray(CacheKV.MapAddress nextAddress, byte[] keyHashCode, int value,
                                         int elementSize) {
        KVElement element = new KVElement(nextAddress, keyHashCode, value);
        element.setElementSize(elementSize);
        return element.getBytes();
    }

    private KVElement(CacheKV.MapAddress nextAddress, byte[] keyHashCode, int value) {
        this.nextAddress = nextAddress;
        this.keyHashCode = new ByteArray(keyHashCode, 0, keyHashCode.length);
        if (!noValue) {
            this.value = new ByteArray(NumberUtils.toByte(value));
        }

    }

    public boolean isEmpty() {
        boolean empytHashCode = true;
        for (int i = 0; i < keyHashCode.getSize(); i++) {
            if (keyHashCode.getByte(i) != 0) {
                empytHashCode = false;
                break;
            }
        }
        return (empytHashCode && nextAddress.isEmpty()) ? true : false;
    }

    public ByteArray getKeyHashCode() {
        return keyHashCode;
    }

    public void setKeyHashCode(ByteArray keyHashCode) {
        this.keyHashCode = keyHashCode;
    }

    public void setKeyHashCode(byte[] keyHashCode) {
        this.keyHashCode = new ByteArray(keyHashCode);
    }

    public byte[] getBytes() {
        byte[] bytes = new byte[elementSize];
        NumberUtils.putSubByte2ByteArray(bytes, 0, nextAddress.createBytes());
        NumberUtils.putSubByte2ByteArray(bytes, 4, keyHashCode.getByteArray());
        if (!noValue) {
            NumberUtils.putSubByte2ByteArray(bytes, 20, value.getByteArray());
        }

        return bytes;
    }

    public void setValue(int value) {
        this.value = new ByteArray(NumberUtils.toByte(value));
    }

    public boolean isNoValue() {
        return noValue;
    }

    public int getElementSize() {
        return elementSize;
    }

    public KVElement setElementSize(int elementSize) {
        this.elementSize = elementSize;
        if (this.elementSize == 20) {
            this.noValue = true;
        }
        return this;
    }

}
