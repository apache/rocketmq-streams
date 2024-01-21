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

    protected KVAddress nextAddress;

    protected ByteArray keyHashCode;

    /**
     * value的值
     */
    protected ByteArray value;

    /**
     * 是否没有value
     */
    protected boolean hasValue = false;

    /**
     * 元素个数
     */
    protected int elementSize = 26;

    public KVElement(ByteArray byteArray, boolean hasValue) {
        if (!hasValue) {
            elementSize = 21;
        } else {
            elementSize = 26;
        }
        this.hasValue = hasValue;
        this.nextAddressByte = byteArray.subByteArray(0, 5);
        nextAddress = new KVAddress(this.nextAddressByte);
        this.keyHashCode = byteArray.subByteArray(5, 16);
        if (hasValue) {
            value = byteArray.subByteArray(21, 5);
        }

    }

    private KVElement(KVAddress nextAddress, byte[] keyHashCode, ByteArray value) {
        this.nextAddress = nextAddress;
        this.keyHashCode = new ByteArray(keyHashCode, 0, keyHashCode.length);
        this.value = value;
        if (value != null) {
            this.hasValue = true;
            this.elementSize = 26;
        }
    }

    public static byte[] createByteArray(KVAddress nextAddress, byte[] keyHashCode, ByteArray value, boolean hasValue) {
        KVElement element = new KVElement(nextAddress, keyHashCode, value);
        if (!hasValue) {
            element.elementSize = 21;
        } else {
            element.elementSize = 26;
        }
        element.hasValue = hasValue;
        return element.getBytes();
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
        NumberUtils.putSubByte2ByteArray(bytes, 5, keyHashCode.getByteArray());
        if (hasValue) {
            NumberUtils.putSubByte2ByteArray(bytes, 21, value.getByteArray());
        }

        return bytes;
    }

    public void setValue(int value) {
        this.value = new ByteArray(NumberUtils.toByte(value));
    }

    public boolean isHasValue() {
        return hasValue;
    }

    public int getElementSize() {
        return elementSize;
    }

}
