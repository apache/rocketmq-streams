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

/**
 * key：list boolean value
 */
public class BitSetCache {
    protected ByteArrayValueKV cache;
    protected int byteSetSize;
    protected int capacity;
    protected int bitSetSize;

    public static class BitSet {
        private byte[] bytes;
        protected int byteSetSize;
        protected int bitSetSize;
        public BitSet(int bitSetSize) {
            this.byteSetSize=bitSetSize/8+(bitSetSize%8==0?0:1);
            this.bitSetSize=bitSetSize;
            bytes = new byte[byteSetSize];
        }

        public BitSet(byte[] bytes) {
            this.bytes = bytes;
            this.byteSetSize=bytes.length;
            this.bitSetSize=this.byteSetSize*8;
        }

        public void set(int index) {
            if (index > bitSetSize) {
                throw new RuntimeException("the index exceed max index, max index is " + byteSetSize + ", real is " + index);
            }
            int bitIndex = index % 8;

            int byteIndex = index/8;
            if(byteIndex>0){
                byteIndex= index / 8+(bitIndex==0?0:1)-1;
            }

            try {
                byte byteElement = bytes[byteIndex];
                byteElement = (byte) (byteElement | (1 << bitIndex));
                bytes[byteIndex] = byteElement;
            }catch (Exception e){
             e.printStackTrace();
            }

        }

        public boolean get(int index) {
            if (index > bitSetSize) {
                throw new RuntimeException("the index exceed max index, max index is " + byteSetSize + ", real is " + index);
            }
            int bitIndex = index % 8;
            int byteIndex = index/8;
            if(byteIndex>0){
                byteIndex= index / 8+(bitIndex==0?0:1)-1;
            }
            byte byteElement = bytes[byteIndex];
            return ((byteElement & (1 << bitIndex)) != 0);

        }

        public byte[] getBytes() {
            return bytes;
        }
    }

    public BitSet createBitSet(int bitSetSize) {
        return new BitSet(bitSetSize);
    }

    public BitSet createBitSet() {
        if(bitSetSize==0){
            throw new RuntimeException("can not support this method");
        }
        return new BitSet(bitSetSize);
    }
    public BitSetCache( int capacity) {
        cache = new ByteArrayValueKV(capacity, byteSetSize);
        this.capacity = capacity;
    }
    public BitSetCache(int bitSetSize, int capacity) {
        this.byteSetSize = bitSetSize / 8 + (bitSetSize % 8==0?0:1);
        this.bitSetSize = bitSetSize;
    }

    public void put(String key, BitSet bitSet) {
        if (cache.size > cache.capacity) {
            synchronized (this) {
                if (cache.size > cache.capacity) {
                    cache = new ByteArrayValueKV(capacity, byteSetSize);
                }
            }
        }
        cache.put(key, bitSet.getBytes());

    }

    public static void main(String[] args) {
        BitSetCache bitSetCache = new BitSetCache(150, 30000);
        BitSet bitSet = bitSetCache.createBitSet(150);
        bitSet.set(13);
        bitSetCache.put("fdsdf", bitSet);
        BitSet bitSet1 = bitSetCache.get("fdsdf");
        System.out.println(bitSet1.get(13));
    }

    public BitSet get(String key) {
        byte[] bytes = cache.get(key);
        if (bytes == null) {
            return null;
        }
        return new BitSet(bytes);

    }

    public long size(){
        return this.cache.getSize();
    }

}
