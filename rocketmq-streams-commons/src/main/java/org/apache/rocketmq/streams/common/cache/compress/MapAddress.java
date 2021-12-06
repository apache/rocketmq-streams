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

public class MapAddress {
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

    public MapAddress(int conflictIndex, int offset) {
        this.isConflict = true;
        this.conflictIndex = conflictIndex;
        this.offset = offset;
    }

    public MapAddress(int offset) {
        this.isConflict = false;
        this.conflictIndex = -1;
        this.offset = offset;
    }

    public MapAddress() {

    }

    public boolean isEmpty() {
        return !isConflict && conflictIndex == 0 && offset == 0;
    }

    /**
     * 高位是0则全部四位代表在map中的偏移量，高位是1则最高位代表冲突域的索引，第三位代表在冲突域链表中的地址
     *
     * @param byteArray
     */
    public MapAddress(ByteArray byteArray) {
        byte firstByte = 0;
        firstByte = byteArray.getByte(byteArray.getSize() - 1);
        //  byte firstByte=byteArray.getByte(byteArray.getSize()-1);
        int conflictValue = NumberUtils.toInt(firstByte);
        int conflictFlag = conflictValue >> 7;
        if (conflictFlag == 1) {
            isConflict = true;
        } else {
            isConflict = false;
            this.offset = byteArray.castInt(0, 4);
            return;
        }
        //TODO 这个地址是不是每次相同？
        this.conflictIndex = conflictValue & 127;
        this.offset = byteArray.castInt(0, 3);
    }

    public byte[] createBytes() {
        byte[] bytes = NumberUtils.toByte(offset);
        int value = 0;
        byte fisrtByte = (byte) (conflictIndex & 0xff);
        if (isConflict) {
            value = (fisrtByte | (1 << 7));//把第一位变成1
        } else {
            return bytes;
        }

        bytes[bytes.length - 1] = (byte) (value & 0xff);
        return bytes;
    }

    public byte[] createBytesIngoreFirstBit() {
        byte[] bytes = NumberUtils.toByte(offset);
        byte fisrtByte = (byte) (conflictIndex & 0xff);
        bytes[bytes.length - 1] = fisrtByte;
        return bytes;
    }
}
