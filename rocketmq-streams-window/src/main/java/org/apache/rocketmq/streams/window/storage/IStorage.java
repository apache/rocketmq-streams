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
 */package org.apache.rocketmq.streams.window.storage;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;

public interface IStorage<T> {


    /**
     * 支持单个key value的存储
     * @param key
     * @param value
     */
    void put(String key, T value);

    //多组key value批量存储
    void mutilPut(Map<String, T> keyValue);

    //获取单个key的值
    T get(Class<T> clazz, IKeyGenerator keyGenerator, String key);

    //获取多个key的值
    Map<String,T> mutilGet(Class<T> clazz, IKeyGenerator keyGenerator, String... keyValues);
    //获取多个key的值
    Map<String,T> mutilGet(Class<T> clazz, IKeyGenerator keyGenerator, List<String> keys);

    /**
     * remove keys
     * @param keys
     */
    void removeKeys(IKeyGenerator keyGenerator, Collection<String> keys);

    /**
     * remove keys by prefix
     * @param keyPrefix
     */
    void removeKeyPrefix(IKeyGenerator keyGenerator, String keyPrefix);

    /*
        create Iterator by key prefix
     */
    Iterator<T> iterateByPrefix(IKeyGenerator keyGenerator, String keyPrefix, Class<T> clazz);


    T putIfAbsent(T t, Class<T> clazz);


    int count(IKeyGenerator keyGenerator, String key);

    int incrementAndGet(IKeyGenerator keyGenerator, String key);


    Iterator<T> queryByPrefixBetweenOrderByValue(IKeyGenerator keyGenerator, String keyPrefix, Object startIndexValue,
        Object endIndexValue, Class<T> clazz);



    void loadByPrefixBetweenOrderByValue(IKeyGenerator keyGenerator, String keyPrefix, Object startIndexValue,
        Object endIndexValue,
        IRowOperator rowOperator, Class<T> clazz);

}
