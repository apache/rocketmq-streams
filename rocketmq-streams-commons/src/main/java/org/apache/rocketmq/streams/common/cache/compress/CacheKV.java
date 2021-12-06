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

import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型
 */
public abstract class CacheKV<T> implements ICacheKV<T> {
    /**
     * 如果存储int作为值，最大值不能超过这个值，如果超过，会改成冲突链的模式
     */
    protected static final int MAX_INT_VALUE = 2147483647;

    /**
     * 发生冲突的次数
     */
    protected int conflictCount;

    protected int elementSize = 24;

    /**
     * 建议设置，如果不设置，默认容量为最大容量
     */
    protected int capacity = 8000000;

    /**
     * 元素个数
     */
    protected int size = 0;

    /**
     * 每一个元素分24个字节部分(下一个地址4个字节，key的md5值16个字节，int的值4个字节)
     */
    protected byte[] map;

    /**
     * 当发生hash冲突时，map值存储在这里
     */
    protected AdditionStore conflicts;

    public CacheKV(int capacity, int elementSize) {
        this.capacity = capacity;
        this.elementSize = elementSize;
        map = new byte[this.capacity * elementSize];
        conflicts = new AdditionStore(elementSize);

    }

    public CacheKV(int capacity) {
        this(capacity, 24);
    }

    @Override
    public abstract T get(String key);

    @Override
    public abstract void put(String key, T value);

    public T remove(String key) {
        if (StringUtil.isEmpty(key)) {
            return null;
        }
        MapElementContext context = queryMapElementByHashCode(key);
        /**
         * TODO:
         * 1 清空MD5
         * 2 前驱的后继=后继
         * 3 当前链接到冲突域的最后一个节点后
         */
        return null;
    }

    public ByteArray getInner(String key) {
        if (key == null) {
            return null;
        }
        MapElementContext context = queryMapElementByHashCode(key);
        if (context.isMatchKey) {
            return context.mapElement.value;
        }
        return null;
    }

    /**
     * address:是否冲突（1位），conflict index（7位），3个字节地址域名
     *
     * @param key
     * @param value
     */
    public boolean putInner(String key, int value, boolean supportUpdate) {
        if (key == null) {
            return false;
        }
        MapElementContext context = null;
        //符合hash值的最后一个next=null的节点或者是和当前key匹配的节点
        context = queryMapElementByHashCode(key);

        KVElement mapElement = context.mapElement;
        //如果没有发生冲突，说明当前节点无被占用，直接写入
        if (!context.isOccurConflict) {
            size++;

            mapElement.keyHashCode.flush(mapElement.getKeyHashCode());
            if (!mapElement.isNoValue()) {
                mapElement.value.flush(value);
            }

            NumberUtils.putSubByte2ByteArray(map, context.mapAddress.offset, mapElement.getBytes());
        } else {
            //如果key已经存在，覆盖value
            if (context.isMatchKey) {
                if (!mapElement.isNoValue()) {
                    if (!supportUpdate) {
                        return false;
                    }
                    mapElement.value.flush(value);
                }
            } else {//如果是新key，且有hash冲突，把新值写入冲突域，把冲突域地址更新next地址
                size++;
                conflictCount++;
                byte[] bytes = KVElement.createByteArray(new MapAddress(), context.keyMd5, value, elementSize);
                MapAddress mapAddress = conflicts.add2Store(bytes);
                context.mapElement.nextAddressByte.flush(mapAddress);

            }
        }
        return true;
    }

    /**
     * 查找同hashcode的冲突链，如果key的值等于当前key值，直接返回。如果key值不同，返回冲突链最后一个元素
     *
     * @param key
     * @return
     */
    public MapElementContext queryMapElementByHashCode(String key) {
        int offset = getElementIndex(key);
        byte[] hashCodes = AESUtil.stringToMD5(key);

        MapAddress address = new MapAddress(offset);//先从map中查找

        MapElementContext context = getMapElementByAddress(hashCodes, address);

        if (context.mapElement.isEmpty()) {//如果map中无值，直接返回
            context.keyMd5 = hashCodes;
            context.mapElement.setKeyHashCode(hashCodes);
            return context;
        } else {
            while (true) {//通过链表找到最后一个节点
                context.keyMd5 = hashCodes;
                if (context.isMatchKey) {
                    return context;
                } else {
                    // System.out.println("occure conflict");
                    context.mapAddress.isConflict = true;
                    if (context.mapElement.nextAddress.isEmpty()) {
                        return context;
                    } else {
                        context = getMapElementByAddress(hashCodes, context.mapElement.nextAddress);
                    }
                }
            }
        }

    }

    @Override
    public int calMemory() {//计算总共分配的内存
        int totalMemory = this.capacity * elementSize + (this.conflicts.getConflictIndex() + 1) * this.conflicts
            .getBlockSize();

        return totalMemory / 1024 / 1024;
    }

    private boolean equalsByte(byte[] hashCodes, ByteArray keyHashCode) {//判读两个hash code是否相等
        if (hashCodes == null || keyHashCode == null) {
            return false;
        }
        if (hashCodes.length != keyHashCode.getSize()) {
            return false;
        }
        for (int i = 0; i < hashCodes.length; i++) {
            if (hashCodes[i] != keyHashCode.getByte(i)) {
                return false;
            }
        }
        return true;

    }

    @Override
    public int getSize() {
        return size;
    }

    /**
     * 查找某个地址对应的map element
     *
     * @param hashCodes
     * @param address
     * @return
     */
    protected MapElementContext getMapElementByAddress(byte[] hashCodes, MapAddress address) {
        if (address.isConflict) {
            ByteArray valueBytes = this.conflicts.getValue(address);
            KVElement mapElement = new KVElement(valueBytes);
            mapElement.setElementSize(this.elementSize);
            return new MapElementContext(mapElement, address, equalsByte(hashCodes, mapElement.keyHashCode));
        } else {
            KVElement mapElement = new KVElement(new ByteArray(map, address.offset, elementSize));
            mapElement.setElementSize(this.elementSize);
            return new MapElementContext(mapElement, address, equalsByte(hashCodes, mapElement.keyHashCode));
        }
    }

    /**
     * 获取map的索引，通过hashcode获取
     *
     * @param key
     * @return
     */
    protected int getElementIndex(String key) {
        if (key == null) {
            return 0;
        }
        int hashCode;
        int value = (hashCode = key.hashCode()) ^ (hashCode >>> 16);
        value = String.valueOf(value).hashCode();
        if (value < 0) {
            value = -value;
        }
        int index = value % capacity;
        return index * elementSize;
    }

    protected class MapElementContext {

        /**
         * 当前的元素
         */
        protected KVElement mapElement;

        /**
         * 元素所在的地址
         */
        protected MapAddress mapAddress;

        /**
         * 是否和key的hashcode 匹配
         */
        protected boolean isMatchKey = false;

        /**
         * 是否发生冲突
         */
        protected boolean isOccurConflict = false;

        /**
         * 本次查询key对应的hash code
         */
        protected byte[] keyMd5;

        public MapElementContext(KVElement mapElement, MapAddress mapAddress, boolean isMatchKey) {
            this.mapAddress = mapAddress;
            this.mapElement = mapElement;
            this.isMatchKey = isMatchKey;
            if (!mapElement.isEmpty()) {
                isOccurConflict = true;
            }
        }

        public boolean isMatchKey() {
            return isMatchKey;
        }
    }


}
