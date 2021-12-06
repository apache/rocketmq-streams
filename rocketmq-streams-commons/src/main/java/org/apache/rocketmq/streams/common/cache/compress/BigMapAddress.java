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

public class BigMapAddress extends MapAddress{

    public BigMapAddress(int conflictIndex, int offset) {
        super(conflictIndex, offset);
    }

    public BigMapAddress(int offset) {
        super(offset);
    }

    public BigMapAddress() {
    }

    /**
     * 高位是0则全部四位代表在map中的偏移量，高位是1则最高位代表冲突域的索引，第三位代表在冲突域链表中的地址
     *
     * @param byteArray
     */
    public BigMapAddress(ByteArray byteArray) {
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

        byte[] bytes=new byte[2];
        bytes[0]=byteArray.getByte(4);
        bytes[1]=NumberUtils.toByte(conflictValue&127)[0];
        this.conflictIndex = NumberUtils.toInt(bytes);
        this.offset = byteArray.castInt(0, 3);
    }

    @Override
    public byte[] createBytes() {
        byte[] values=new byte[5];
        byte[] offsetBytes = NumberUtils.toByte(offset);
        for(int i=0;i<3;i++){
            values[i]=offsetBytes[i];
        }

        byte[] indexBytes=NumberUtils.toByte(conflictIndex);
        values[3]=indexBytes[0];
        byte fisrtByte =indexBytes[1];

        int value = 0;
        if (isConflict) {
            value = (fisrtByte | (1 << 7));//把第一位变成1
        } else {
            return values;
        }

        values[4] = (byte) (value & 0xff);
        return  values;
    }

    @Override
    public byte[] createBytesIngoreFirstBit() {
        byte[] values=new byte[5];
        byte[] offsetBytes = NumberUtils.toByte(offset);
        for(int i=0;i<3;i++){
            values[i]=offsetBytes[i];
        }

        byte[] indexBytes=NumberUtils.toByte(conflictIndex);
        values[3]=indexBytes[0];
        values[4]=indexBytes[1];
        return values;
    }
}
