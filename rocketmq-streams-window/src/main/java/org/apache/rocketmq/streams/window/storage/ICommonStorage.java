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
package org.apache.rocketmq.streams.window.storage;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * 对存储的统一抽象,最底层的抽象 T必须是可识别的对象
 */
public interface ICommonStorage<T> {

    /**
     * 支持单个key value的存储
     *
     * @param key
     * @param value
     */
    void put(String key, T value);

    //多组key value批量存储
    void multiPut(Map<String, T> map);

    /**
     * put <key,list> into the storage
     *
     * @param elements
     */
    void multiPutList(Map<String, List<T>> elements);

    //获取多个key的值
    Map<String, T> multiGet(Class<T> clazz, List<String> keys);

    /**
     * get list from storage according by key
     *
     * @param clazz
     * @param keys
     * @return
     */
    Map<String, List<T>> multiGetList(Class<T> clazz, List<String> keys);

    //获取单个key的值
    T get(Class<T> clazz, String key);

    //获取多个key的值
    Map<String, T> multiGet(Class<T> clazz, String... keys);

    void removeKeys(Collection<String> keys);

}
