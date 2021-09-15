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
 * kv提供的对外接口,通过二进制实现存储，减少java对象带来的头部开销。 需要指定初始容量，会在创建对象时分配内存。
 */
public interface ICacheKV<T> {

    /**
     * 根据key获取value
     *
     * @param key
     * @return
     */
    T get(String key);

    /**
     * 把key，value插入到kv中
     *
     * @param key
     * @param value
     */
    void put(String key, T value);

    /**
     * 是否包含
     *
     * @param key
     * @return
     */
    boolean contains(String key);

    /**
     * 一共有多少行
     *
     * @return
     */
    int getSize();

    /**
     * 占用多少内存
     *
     * @return
     */
    int calMemory();

}
