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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.AdditionStore;
import org.apache.rocketmq.streams.common.cache.compress.ByteArray;
import org.apache.rocketmq.streams.common.cache.compress.CacheKV;
import org.apache.rocketmq.streams.common.cache.compress.MapAddress;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public abstract class AbstractListKV<T> extends CacheKV<List<T>> {

    public AbstractListKV(int capacity, int elementSize) {
        super(capacity, elementSize);
    }

    public AbstractListKV(int capacity) {
        super(capacity);
    }

    @Override
    public List<T> get(String key) {
        List<T> values=new ArrayList<>();
        ByteArray byteArray=getLastElement(key,values);
        if(byteArray==null){
            return null;
        }
        return values;
    }

    /**
     * 增加一个元素
     * @param key
     * @param value
     * @return
     */
    public synchronized List<T> add(String key,T value){
        List<T> values=new ArrayList<>();
        ByteArray last=getLastElement(key,values);
        add(key,value,last);
        values.add(value);
        return values;
    }


    protected abstract byte[] convertByte(T value);

    protected abstract int getElementSize();

    protected abstract AdditionStore getValues();

    /**
     * 获取最后一个元素
     * @param key
     * @param values
     * @return
     */
    protected ByteArray getLastElement(String key,List<T> values){
        return getLastElement(key,values,null);
    }
    /**
     * 获取最后一个元素
     * @param key
     * @param values
     * @return
     */
    protected abstract ByteArray getLastElement(String key,List<T> values,ListValueAddress addresses );


    protected void add(String key,T value, ByteArray last){
        if(key==null||value==null){
            return;
        }
        /**
         * 当前元素是第一个元素，插入到父类的conflict字段
         */
        if(last==null){
            byte[] element=createElement(convertByte(value));
            MapAddress address=this.getValues().add2Store(element);
            byte[] addressByte=address.createBytes();
            super.putInner(key, NumberUtils.toInt(addressByte), true);
            return;
        }
        byte[] element=createElement(convertByte(value));
        MapAddress address=this.getValues().add2Store(element);
        byte[] nextAddress=address.createBytes();
        for(int i=0;i<nextAddress.length;i++){
            last.setByte(i+getElementSize(),nextAddress[i]);
        }
    }


    private static byte[] ZERO_BYTE=NumberUtils.toByte(0);
    /**
     * 创建一个元素，包含两部分：int值，int
     * @return
     */
    protected byte[] createElement(byte[] intValueByte) {
        byte[] element=new byte[4+getElementSize()];
        for(int j=0;j<getElementSize();j++){
            element[j]=intValueByte[j];
        }
        for(int j=0;j<4;j++){
            element[j+getElementSize()]=ZERO_BYTE[j];
        }
        return element;
    }
    @Override
    public void put(String key, List<T> values) {
        throw new RuntimeException("can not use this method, please use add method");
    }

    @Override
    public boolean contains(String key) {
        List<T> value = get(key);
        if (value == null) {
            return false;
        }
        return true;

    }

    @Override
    public int calMemory() {
        return super.calMemory() + ((this.getValues().getConflictIndex() + 1) * this.getValues().getBlockSize()/1024/1024);
    }


    class ListValueAddress{
        protected ByteArray header;
        protected List<MapAddress> addresses=new ArrayList<>();

        public void setHeader(ByteArray header) {
            this.header = header;
        }

        public void addAddress(MapAddress address){
            addresses.add(address);
        }
    }
}


