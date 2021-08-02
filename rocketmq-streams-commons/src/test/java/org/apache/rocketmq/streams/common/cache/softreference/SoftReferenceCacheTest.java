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
package org.apache.rocketmq.streams.common.cache.softreference;

import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.RebuildCacheElement;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.junit.Test;

/**
 * 基于软引用实现的缓存，可以在内存不足时回收，尽量最大限度使用内存的场景使用 对象被内存回收后，get返回null，或通过设置RebuildCacheElement接口，对象回收后会自动创建
 */
public class SoftReferenceCacheTest {

    @Test
    public void testSoftReferenceCache() {
        //空的构造方法，对象被放入cache后，有可能被回收，回收后，会返回null
        ICache<String, String> cache = new SoftReferenceCache<>();
        cache.put("name", "chris");
        String value = cache.get("name");
        if (value == null) {
            cache.put("name", "chris");
        }
        System.out.println(value);

    }

    @Test
    public void testSoftReferenceCacheRebuildCacheElement() {
        //构造方法传人RebuildCacheElement接口，在对象被回收后，会自动创建，只要存放过对象，就确保能拿到
        ICache<String, String> cache = new SoftReferenceCache<>(new RebuildCacheElement<String, String>() {
            @Override
            public String create(String s) {
                return "chris";
            }
        });
        cache.put("name", "chris");
        String value = cache.get("name");
        System.out.println(value);
    }
}
