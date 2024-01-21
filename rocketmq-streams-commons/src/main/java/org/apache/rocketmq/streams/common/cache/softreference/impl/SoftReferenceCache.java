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
package org.apache.rocketmq.streams.common.cache.softreference.impl;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.RebuildCacheElement;

/**
 * 基于软引用实现的缓存，可以在内存不足时回收，尽量最大限度使用内存的场景使用
 */
public class SoftReferenceCache<K, V> implements ICache<K, V> {

    protected RebuildCacheElement<K, V> rebuildCacheElement;
    // 缓存，用软引用记录
    private ConcurrentHashMap<K, ExtraInfoReference<V>> cache = new ConcurrentHashMap<K, ExtraInfoReference<V>>();
    private ReferenceQueue<V> refQueue = new ReferenceQueue<V>();

    public SoftReferenceCache(RebuildCacheElement<K, V> rebuildCacheElement) {
        this.rebuildCacheElement = rebuildCacheElement;
    }

    public SoftReferenceCache() {
        this.rebuildCacheElement = new RebuildCacheElement<K, V>() {
            @Override
            public V create(K k) {
                return null;
            }
        };
    }

    @Override
    public V get(K key) {
        V value = null;

        if (cache.containsKey(key)) {
            SoftReference<V> refValue = cache.get(key);
            if (refValue != null) {
                value = refValue.get();
            }

        }
        // 如果软引用被回收
        if (value == null) {
            // 清除软引用队列
            clearRefQueue();
            // 创建值并放入缓存
            value = rebuildCacheElement.create(key);
            if (value != null) {
                ExtraInfoReference<V> refValue = new ExtraInfoReference<V>(key, value, refQueue);
                cache.put(key, refValue);
            }
        }

        return value;
    }

    @Override
    public void put(K k, V v) {
        if (v == null) {
            cache.remove(k);
        }
        ExtraInfoReference<V> refValue = new ExtraInfoReference<V>(k, v, refQueue);
        cache.put(k, refValue);
    }

    @Override
    public V remove(K k) {
        return null;
    }

    @Override
    public List<V> removeByPrefix(K k) {
        return null;
    }

    public void clear() {
        clearRefQueue();
        cache.clear();
    }

    /**
     * 从软引用队列中移除无效引用， 同时从cache中删除无效缓存
     */
    @SuppressWarnings("unchecked")
    protected void clearRefQueue() {
        ExtraInfoReference<V> refValue = null;
        while ((refValue = (ExtraInfoReference<V>) refQueue.poll()) != null) {
            K key = (K) refValue.getExtraInfo();
            cache.remove(key);
        }
    }

    protected class ExtraInfoReference<T> extends SoftReference<T> {

        private Object info;

        public ExtraInfoReference(Object info, T t, ReferenceQueue<T> refQueue) {
            super(t, refQueue);
            this.info = info;
        }

        public Object getExtraInfo() {
            return this.info;
        }
    }

}
