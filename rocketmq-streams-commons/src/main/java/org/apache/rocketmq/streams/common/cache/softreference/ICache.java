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

import java.util.List;

/**
 * 基于软引用实现的缓存，可以在内存不足时回收，尽量最大限度使用内存的场景使用
 */
public interface ICache<K, V> {

    /**
     * get driver
     *
     * @param k
     * @return
     */
    V get(K k);

    /**
     * put driver
     *
     * @param k
     * @param v
     */
    void put(K k, V v);

    /**
     * remove driver which return the removed value
     *
     * @param k
     * @return
     */
    V remove(K k);

    /**
     * remove all keys which has same prefix and return the removed value in the list
     *
     * @param k
     * @return
     */
    List<V> removeByPrefix(K k);

}
