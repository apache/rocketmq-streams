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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class AdditionStore {

    /**
     * 每个冲突域列表，每个列表的最大值
     */
    public static final int CONFLICT_UNIT_SIZE = 16777216;

    /**
     * 如果value是非int值，可以通过这个值存储。原来value部分存储地址
     */
    private List<byte[]> values = new ArrayList<>();

    /**
     * 当前冲突元素存放在list的哪个index中
     */
    private int conflictIndex = -1;

    /**
     * 当前冲突的元素在byte中的可用位置
     */
    private int conflictOffset = 0;

    /**
     * 如果元素是固定大小，则这个值表示元素字节个数；
     */
    private int elementSize = -1;

    /**
     * 值是否是变长的，如果是变长的，需要额外两个字段存储长度
     */
    private boolean isVarLen = true;

    /**
     * 每个存储单元的大小
     */
    protected int blockSize = CONFLICT_UNIT_SIZE;


    public AdditionStore(int elementSize, int blockSize) {
        this.elementSize = elementSize;
        if (elementSize > 0) {
            isVarLen = false;
        }
        if (blockSize > 0) {
            this.blockSize = blockSize;
        }
    }

    public AdditionStore(int elementSize) {
        this(elementSize, CONFLICT_UNIT_SIZE);
    }

    public class DataElement{
        protected byte[] bytes;
        protected MapAddress mapAddress;
        public DataElement(byte[] bytes,MapAddress mapAddress){
            this.bytes=bytes;
            this.mapAddress=mapAddress;
        }

        public byte[] getBytes() {
            return bytes;
        }

        public MapAddress getMapAddress() {
            return mapAddress;
        }
    }

    public Iterator<DataElement> iterator(){
        return new Iterator<DataElement>(){
            int index=0;
            int offset=0;
            @Override public boolean hasNext() {
                if(index<conflictIndex){
                    return true;
                }
                if(offset<conflictOffset){
                    return true;
                }
                return false;
            }

            @Override public DataElement next() {
                MapAddress address=new MapAddress(index,offset);
                ByteArray byteArray=getValue(address);
                if(byteArray==null&&hasNext()){
                    this.index++;
                    this.offset=0;
                    return next();
                }
                byte[] bytes= byteArray.getByteArray();
                offset=offset+bytes.length+2;
                if(offset>blockSize||(isVarLen==false&&offset+elementSize>blockSize)){
                    this.index++;
                    this.offset=0;
                }
                return new DataElement(bytes,address);
            }
        };


    }

    /**
     * 把一个字节数组插入到存储中，并返回当前地址
     *
     * @param value
     */
    public MapAddress add2Store(byte[] value) {
        if (conflictIndex == -1 || values.size() <= conflictIndex) {
            byte[] bytes = new byte[blockSize];
            values.add(bytes);
            conflictOffset = 0;
        }
        if (conflictIndex == -1) {
            conflictIndex = 0;
        }
        int length = value.length;
        if (isVarLen) {
            length = length + 2;
        }
        if (conflictOffset + length > blockSize) {
            byte[] bytes = new byte[blockSize];
            values.add(bytes);
            conflictOffset = 0;
            conflictIndex++;
            if(conflictIndex>127){
                throw new RuntimeException("exceed cache size "+conflictIndex);
            }
        }

        byte[] bytes = values.get(conflictIndex);

        MapAddress address = new MapAddress(conflictIndex, conflictOffset);
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
            }

        }
        conflictOffset += value.length;
        return address;
    }

    /**
     * 根据当前地址获取对应的byte值
     *
     * @param mapAddress
     * @return
     */
    public ByteArray getValue(MapAddress mapAddress) {
        byte[] bytes = values.get(mapAddress.conflictIndex);
        if (bytes == null) {
            return null;
        }
        if (!isVarLen) {
            return new ByteArray(bytes, mapAddress.offset, elementSize);
        } else {
            if(mapAddress.offset+2>bytes.length){
                return null;
            }
            int len = new ByteArray(bytes, mapAddress.offset, 2).castInt(0, 2);
            if(len==0){
                return null;
            }
            return new ByteArray(bytes, mapAddress.offset + 2, len);
        }
    }

    public int byteSize(){
        long byteSize=this.blockSize*this.conflictIndex+this.blockSize;
        return (int)(byteSize)/1024/1024;
    }

    public int getConflictIndex() {
        return conflictIndex;
    }

    public int getConflictOffset() {
        return conflictOffset;
    }

    public int getBlockSize() {
        return blockSize;
    }

    public void setConflictIndex(int conflictIndex) {
        this.conflictIndex = conflictIndex;
    }

    public void setConflictOffset(int conflictOffset) {
        this.conflictOffset = conflictOffset;
    }
}
