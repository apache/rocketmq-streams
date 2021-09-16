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

import java.util.Arrays;

public class ByteArray {

    private byte[] bytes;
    private int startIndex;
    private int size;

    public ByteArray(ByteArray byteArray, int startIndex, int size) {
        this.bytes = byteArray.bytes;
        this.startIndex = byteArray.startIndex + startIndex;
        this.size = size;
    }

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
        this.startIndex = 0;
        this.size = bytes.length;
    }

    public ByteArray(byte[] bytes, int startIndex, int size) {
        this.bytes = bytes;
        this.startIndex = startIndex;
        this.size = size;
    }

    public ByteArray subByteArray(int offset, int size) {
        return new ByteArray(bytes, startIndex + offset, size);
    }

    public byte[] getByteArray() {
        return Arrays.copyOfRange(bytes, startIndex, startIndex + size);
    }

    public int castInt(int offset, int size) {
        int index = startIndex + offset;
        int res = 0;
        for (int i = 0; i < size; i++) {
            res += (bytes[i + index] & 0xff) << (i * 8);
        }
        return res;
    }

    public long castLong(int offset, int size) {
        int index = startIndex + offset;
        long res = 0L;
        for (int i = 0; i < size; i++) {
            res += (long) (bytes[i + index] & 0xff) << (i * 8);
        }
        return res;
    }

    public byte getByte(int offset) {
        int index = startIndex + offset;
        return bytes[index];
    }

    public int getSize() {
        return size;
    }

    public void flush(CacheKV.MapAddress address) {
        if (address == null) {
            return;
        }
        if (address.isConflict == false) {
            flush(address.offset);
        } else {
            flush(address.createBytes());
        }
    }

    protected void flush(ByteArray bytes) {
        for (int i = 0; i < bytes.size; i++) {
            this.bytes[i + this.startIndex] = bytes.getByte(i);
        }
    }

    protected void flush(byte[] bytes) {
        for (int i = 0; i < bytes.length; i++) {
            this.bytes[i + this.startIndex] = bytes[i];
        }
    }

    protected void flush(int value) {
        for (int i = 0; i < 4; i++) {
            if (i == 0) {
                this.bytes[i + this.startIndex] = (byte) (value & 0xff);
            } else {
                this.bytes[i + this.startIndex] = (byte) (value >> (i * 8) & 0xff);
            }
        }
    }
}
