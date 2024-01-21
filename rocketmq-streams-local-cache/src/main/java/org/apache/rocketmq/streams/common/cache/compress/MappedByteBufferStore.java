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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class MappedByteBufferStore extends ByteStore {
    protected String filePath;
    protected transient MappedByteBuffer buffer;//内存映射的对象
    protected transient Map<Integer, Long> index2BufferStartIndex = new HashMap<>();
    protected transient byte[] currentBytes;

    public MappedByteBufferStore(String filePath, int elementSize, int blockSize) {
        super(elementSize, blockSize);
        this.filePath = filePath;
        this.buffer = init(filePath);
    }

    public MappedByteBufferStore(String filePath, int elementSize) {
        super(elementSize);
        this.filePath = filePath;
        buffer = init(filePath);
    }

    public static void main(String[] args) {
        MappedByteBufferStore mappedByteBufferStore = new MappedByteBufferStore("/tmp/memory.txt", 10);
    }

    private MappedByteBuffer init(String filePath) {

        MappedByteBuffer out = null;

        try {
            out = new RandomAccessFile(this.filePath, "rw").getChannel()
                .map(FileChannel.MapMode.READ_WRITE, 0, 512 * 1024 * 1024 * 1024);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return out;
    }

    /**
     * 把一个字节数组插入到存储中，并返回当前地址
     *
     * @param value
     */
    @Override
    public synchronized KVAddress add2Store(byte[] value) {
        if (conflictIndex == 0 || conflictIndex == -1 || this.currentBytes == null) {
            this.currentBytes = createBytesForIndex();

        }

        int length = value.length;
        if (isVarLen) {
            length = length + 2;
        }
        if (conflictOffset + length > blockSize) {
            if ((conflictIndex + 1) > 32767) {
                throw new RuntimeException("exceed cache size " + (conflictIndex + 1));
            }
            flushBytes(this.currentBytes);
            this.currentBytes = createBytesForIndex();
        }

        byte[] bytes = this.currentBytes;

        KVAddress address = new KVAddress(conflictIndex, conflictOffset);
        if (isVarLen) {
            int size = value.length;
            bytes[conflictOffset] = (byte) (size & 0xff);
            bytes[conflictOffset + 1] = (byte) (size >> 8 & 0xff);
            conflictOffset = conflictOffset + 2;
        }
        for (int i = 0; i < value.length; i++) {
            try {
                bytes[i + conflictOffset] = value[i];
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        }
        conflictOffset += value.length;
        return address;
    }

    protected void flushBytes(byte[] bytes) {
        this.buffer.put(bytes);
    }

    /**
     * 根据当前地址获取对应的byte值
     *
     * @param mapAddress
     * @return
     */
    @Override
    public synchronized ByteArray getValue(KVAddress mapAddress) {
        if (this.conflictIndex == mapAddress.getConflictIndex()) {
            byte[] bytes = this.currentBytes;
            if (!isVarLen) {
                return new ByteArray(bytes, mapAddress.offset, elementSize);
            } else {
                if (mapAddress.offset + 2 > bytes.length) {
                    return null;
                }
                int len = new ByteArray(bytes, mapAddress.offset, 2).castInt(0, 2);
                if (len == 0) {
                    return null;
                }
                return new ByteArray(bytes, mapAddress.offset + 2, len);
            }
        } else {
            Long startIndex = this.index2BufferStartIndex.get(mapAddress.getConflictIndex());
            int index = startIndex.intValue() + mapAddress.conflictIndex;
            int length = -1;
            if (!isVarLen) {
                length = elementSize;
                return new ByteArray(readData(index, length));
            } else {
                Long rowSize = this.index2BufferStartIndex.get(mapAddress.getConflictIndex() + 1);
                if (rowSize != null) {
                    if (mapAddress.offset + 2 > rowSize) {
                        return null;
                    }
                }
                byte[] bytes = readData(index, 2);
                if (bytes == null) {
                    return null;
                }
                int len = NumberUtils.toInt(bytes);
                if (len == 0) {
                    return null;
                }
                bytes = readData(index + 2, length);
                if (bytes == null) {
                    return null;
                }
                return new ByteArray(bytes);
            }
        }

    }

    public byte[] readData(int offset, int size) {
        byte[] bytes = new byte[size];
        int endIndex = offset + size;
        for (int i = offset; i < endIndex; i++) {
            bytes[i - offset] = this.buffer.get(i);
        }
        return bytes;
    }

    protected byte[] createBytesForIndex() {
        int index = this.conflictIndex;
        if (index <= 0) {
            this.index2BufferStartIndex.put(0, 0L);
            this.conflictIndex = 0;
            conflictOffset = 0;
            return new byte[blockSize];
        }
        long cacheIndex = this.index2BufferStartIndex.get(index);
        this.index2BufferStartIndex.put(index + 1, cacheIndex + this.conflictIndex);
        conflictOffset = 0;
        conflictIndex++;
        return new byte[blockSize];

    }

}
