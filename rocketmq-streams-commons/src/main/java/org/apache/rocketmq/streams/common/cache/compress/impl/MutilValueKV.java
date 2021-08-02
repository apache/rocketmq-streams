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
import org.apache.rocketmq.streams.common.cache.compress.ICacheKV;

public abstract class MutilValueKV<T> implements ICacheKV<T> {
    //按固定大小分割存储
    protected List<ICacheKV<T>> valueKVS=new ArrayList<>();
    //当前存储的索引
    protected int currentIndex=0;
    //每个分片的大小
    protected int capacity;

    public MutilValueKV(int capacity){
        this.capacity=capacity;
    }


    @Override
    public T get(String key) {
        if(valueKVS==null){
            return null;
        }
        for(ICacheKV<T> cacheKV:valueKVS){
            if(cacheKV!=null){
               T value=cacheKV.get(key);
                if(value!=null){
                    return value;
                }
            }
        }
        return null;
    }

    @Override
    public void put(String key, T value) {
        if(valueKVS==null){
            return;
        }
        ICacheKV<T> cacheKV= valueKVS.get(currentIndex);
        if(cacheKV.getSize()>=capacity){
            synchronized (this){
                cacheKV= valueKVS.get(currentIndex);
                if(cacheKV.getSize()>=capacity){
                    cacheKV=create();
                    valueKVS.add(cacheKV);
                    currentIndex++;
                }
            }
        }
        cacheKV.put(key,value);
    }

    @Override
    public boolean contains(String key) {
        if(valueKVS==null){
            return false;
        }
        for(ICacheKV<T> cacheKV:valueKVS){
            if(cacheKV!=null){
                boolean isMatch=cacheKV.contains(key);
                if(isMatch){
                    return true;
                }
            }
        }
        return false;
    }


    protected abstract ICacheKV<T> create();

    @Override
    public int getSize() {
        return 0;
    }

    @Override
    public int calMemory() {
        return 0;
    }

    public int getCurrentIndex() {
        return currentIndex;
    }

    public void setCurrentIndex(int currentIndex) {
        this.currentIndex = currentIndex;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }
}
