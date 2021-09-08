package org.apache.rocketmq.streams.common.cache.compress;

import org.apache.rocketmq.streams.common.utils.NumberUtils;

/**
 * key：list boolean value
 */
public class BitSetCache {
    protected ByteArrayValueKV cache;
    protected int byteSetSize;
    protected int capacity;
    protected int bitSetSize;

    public class BitSet{
        private byte[] bytes;

        public BitSet(){
            bytes=new byte[byteSetSize];
        }
        public BitSet(byte[] bytes){
            this.bytes=bytes;
        }
        public void set(int index){
            if(index>bitSetSize){
                throw new RuntimeException("the index exceed max index, max index is "+byteSetSize+", real is "+index);
            }
            int byteIndex=index/8;
            int bitIndex=index%8;
            byte byteElement=bytes[byteIndex];
            byteElement = (byte) (byteElement|(1 << bitIndex));
            bytes[byteIndex]=byteElement;
        }
        public boolean get(int index){
            if(index>bitSetSize){
                throw new RuntimeException("the index exceed max index, max index is "+byteSetSize+", real is "+index);
            }
            int byteIndex=index/8;
            int bitIndex=index%8;
            byte byteElement=bytes[byteIndex];
            boolean isTrue = ((byteElement & (1 << bitIndex)) != 0);
            return isTrue;
        }

        public byte[] getBytes(){
            return bytes;
        }
    }

    public BitSet createBitSet(){
        return new BitSet();
    }


    public BitSetCache(int bitSetSize, int capacity){
        cache=new ByteArrayValueKV(capacity,true);
        this.byteSetSize=bitSetSize/8+bitSetSize%8;
        this.capacity=capacity;
        this.bitSetSize=bitSetSize;
    }


    public void put(String key,BitSet bitSet){
        if(cache.size>cache.capacity){
            synchronized (this){
                if(cache.size>cache.capacity){
                    cache=new ByteArrayValueKV(capacity,true);
                }
            }
        }
        cache.put(key,bitSet.getBytes());

    }

    public static void main(String[] args) {
        BitSetCache bitSetCache=new BitSetCache(150,30000);
        BitSet bitSet=bitSetCache.createBitSet();
        bitSet.set(13);
        bitSetCache.put("fdsdf",bitSet);
        BitSet bitSet1=bitSetCache.get("fdsdf");
        System.out.println(bitSet1.get(13));
    }

    public BitSet get(String key){
        byte[] bytes=cache.get(key);
        if(bytes==null){
            return null;
        }
       return new BitSet(bytes);

    }

    public long size(){
        return this.cache.getSize();
    }

}
