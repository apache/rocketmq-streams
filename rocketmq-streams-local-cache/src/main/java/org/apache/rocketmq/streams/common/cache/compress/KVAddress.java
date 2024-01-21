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

public class KVAddress {
    /**
     * 是否在冲突域
     */
    protected boolean isConflict = false;

    /**
     * 冲突域的list index
     */
    protected int conflictIndex = 0;

    /**
     * list byte【】对应的offset
     */
    protected int offset = 0;

    public KVAddress(int conflictIndex, int offset) {
        this.isConflict = true;
        this.conflictIndex = conflictIndex;
        this.offset = offset;
    }

    public KVAddress(int offset) {
        this.isConflict = false;
        this.conflictIndex = -1;
        this.offset = offset;
    }

    public KVAddress() {

    }

    /**
     * 高位是0则全部四位代表在map中的偏移量，高位是1则最高位代表冲突域的索引，第三位代表在冲突域链表中的地址
     *
     * @param byteArray
     */
    public KVAddress(ByteArray byteArray) {

        byte firstByte = 0;
        firstByte = byteArray.getByte(byteArray.getSize() - 1);
        //  byte firstByte=byteArray.getByte(byteArray.getSize()-1);
        int conflictValue = NumberUtils.toInt(firstByte);
        int conflictFlag = conflictValue >> 7;
        if (conflictFlag == 1) {
            isConflict = true;
        } else {
            isConflict = false;
            this.offset = byteArray.castInt(0, 3);
            return;
        }

        byte[] bytes = new byte[2];
        bytes[0] = byteArray.getByte(3);
        int x = conflictValue & 127;
        bytes[1] = NumberUtils.toByte(x)[0];
        this.conflictIndex = NumberUtils.toInt(bytes);
        this.offset = byteArray.castInt(0, 3);
    }

    public static KVAddress createMapAddressFromLongValue(Long value) {
        byte[] bytes = NumberUtils.toByte(value);
        return createAddressFromBytes(bytes);
    }

    public static KVAddress createAddressFromBytes(byte[] bytes) {
        int offset = NumberUtils.toInt(bytes, 0, 3);
        byte firstByte = bytes[4];
        if (firstByte < 0) {
            bytes[4] = 0;
        }
        int index = NumberUtils.toInt(bytes, 3, 2);
        KVAddress mapAddress = new KVAddress(index, offset);
        return mapAddress;

    }

    public static void main(String[] args) {
//        System.out.println(1L << 63);
//        System.out.println(0x8000000000000000L);

        KVAddress kvAddress = new KVAddress(new ByteArray(NumberUtils.toByte((long) Integer.MAX_VALUE + 1L)));
        System.out.println(kvAddress);

    }

    public boolean isEmpty() {
        return !isConflict && conflictIndex == 0 && offset == 0;
    }

    public byte[] createBytes() {
        byte[] values = NumberUtils.toByte(offset, 5);
        byte[] indexBytes = NumberUtils.toByte(conflictIndex);
        values[3] = indexBytes[0];
        byte fisrtByte = indexBytes[1];

        int value = 0;
        if (isConflict) {
            value = (fisrtByte | (1 << 7));//把第一位变成1
        } else {
            return values;
        }

        values[4] = (byte) (value & 0xff);
        return values;
    }

    public byte[] createBytesIngoreFirstBit() {
        byte[] values = NumberUtils.toByte(offset, 5);
        byte[] indexBytes = NumberUtils.toByte(conflictIndex);
        values[3] = indexBytes[0];
        values[4] = indexBytes[1];
        return values;
    }

    public Long convertLongValue() {
        byte[] bytes = createBytesIngoreFirstBit();
        return NumberUtils.toLong(bytes);

    }

    public int getConflictIndex() {
        return conflictIndex;
    }

    public int getOffset() {
        return offset;
    }
}
