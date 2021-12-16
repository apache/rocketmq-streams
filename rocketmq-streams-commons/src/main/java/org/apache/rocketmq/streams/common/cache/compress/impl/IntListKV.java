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

import java.util.List;
import org.apache.rocketmq.streams.common.cache.compress.AdditionStore;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.MapAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class IntListKV extends AbstractListKV<Integer>{

    protected AdditionStore values = new AdditionStore(8);

    public IntListKV(int capacity) {
        super(capacity);
    }

    @Override protected byte[] convertByte(Integer value) {
        return NumberUtils.toByte(value);
    }

    @Override protected int getElementSize() {
        return 4;
    }

    @Override protected AdditionStore getValues() {
        return values;
    }

    @Override
    protected void add(String key,Integer value, ByteArray last){
        if(key==null||value==null){
            return;
        }
        /**
         * 当前元素是第一个元素，插入到父类的conflict字段
         */
        if(last==null){
            ByteArray byteArray=new ByteArray(NumberUtils.toByte(value));
            if(NumberUtils.isFirstBitZero(byteArray)){
                super.putInner(key, value, true);
                return;
            }
        }
        /**
         * 原来有一个元素，在父类的conflict，需要把这个值移到本类的value字段
         */
        if(NumberUtils.isFirstBitZero(last)&&last.getSize()==4){
            byte[] element= createElement(last);
            MapAddress address=this.values.add2Store(element);
            byte[] addressByte=address.createBytes();
            super.putInner(key, NumberUtils.toInt(addressByte), true);
            add(key,value);
            return;
        }
        byte[] element=createElement(NumberUtils.toByte(value));
        MapAddress address=this.values.add2Store(element);
        byte[] nextAddress=address.createBytes();
        for(int i=0;i<nextAddress.length;i++){
           last.setByte(i+4,nextAddress[i]);
        }
    }



    /**
     * 创建一个元素，包含两部分：int值，int
     * @param value
     * @return
     */
    private byte[] createElement(ByteArray value) {
        return createElement(value.getByteArray());
    }



    public static void main(String[] args) {
        IntListKV intListKV=new IntListKV(1000000);
        List<Integer> values=null;
        intListKV.add("name",1);
        intListKV.add("name",2);
        intListKV.add("name",3);
        intListKV.add("name",4);
        intListKV.add("age",1);
        intListKV.add("age",2);
        values=intListKV.get("age");
        for(int value:values){
            System.out.println(value);
        }
    }
    /**
     * 获取最后一个元素
     * @param key
     * @param values
     * @return
     */
    @Override
    protected ByteArray getLastElement(String key,List<Integer> values,ListValueAddress addresses ){
        ByteArray byteArray = super.getInner(key);
        if (byteArray == null) {
            return null;
        }
        if(addresses!=null){
            addresses.setHeader(byteArray);
        }

        if(NumberUtils.isFirstBitZero(byteArray)){
            int value = byteArray.castInt(0, 4);
            values.add(value);
            return byteArray;
        }
        MapAddress nextAddress= new MapAddress(byteArray);
        ByteArray nextAddressAndValue =null;
        while (!nextAddress.isEmpty()){
            if(addresses!=null){
                addresses.addAddress(nextAddress);
            }
            nextAddressAndValue = this.values.getValue(nextAddress);
            int value=nextAddressAndValue.subByteArray(0,4).castInt(0,4);
            ByteArray address=nextAddressAndValue.subByteArray(4,4);
            nextAddress=new MapAddress(address);
            values.add(value);
        }
        return nextAddressAndValue;
    }
}
