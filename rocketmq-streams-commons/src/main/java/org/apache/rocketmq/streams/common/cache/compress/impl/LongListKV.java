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
package org.apache.rocketmq.streams.common.cache.compress.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.cache.compress.AdditionStore;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.CacheKV;
import org.apache.rocketmq.streams.common.cache.compress.MapAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class LongListKV extends AbstractListKV<Long> {

    protected AdditionStore values = new AdditionStore(12);

    public LongListKV(int capacity, int elementSize) {
        super(capacity, elementSize);
    }

    public LongListKV(int capacity) {
        super(capacity);
    }

    public static void main(String[] args) {
        LongListKV intListKV=new LongListKV(1000000);
        List<Long> values=null;
        intListKV.add("name",1L);
        intListKV.add("name",2L);
        intListKV.add("name",3L);
        intListKV.add("name",4L);
        intListKV.add("age",1L);
        intListKV.add("age",2L);
        values=intListKV.get("age");
        for(long value:values){
            System.out.println(value);
        }
    }

    @Override protected byte[] convertByte(Long value) {
        return NumberUtils.toByte(value);
    }

    @Override protected int getElementSize() {
        return 8;
    }

    @Override protected AdditionStore getValues() {
        return values;
    }

    /**
     * 获取最后一个元素
     * @param key
     * @param values
     * @return
     */
    @Override
    protected ByteArray getLastElement(String key,List<Long> values,ListValueAddress addresses ){
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        if(addresses!=null){
            addresses.setHeader(byteArray);
        }
        MapAddress nextAddress= new MapAddress(byteArray);
        ByteArray nextAddressAndValue =null;
        while (!nextAddress.isEmpty()){
            if(addresses!=null){
                addresses.addAddress(nextAddress);
            }
            nextAddressAndValue = this.values.getValue(nextAddress);
            long value=nextAddressAndValue.subByteArray(0,8).castLong(0,8);
            ByteArray address=nextAddressAndValue.subByteArray(8,4);
            nextAddress=new MapAddress(address);
            values.add(value);
        }
        return nextAddressAndValue;
    }
}
