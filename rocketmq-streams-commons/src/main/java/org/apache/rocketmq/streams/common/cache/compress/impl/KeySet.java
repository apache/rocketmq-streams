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

import org.apache.rocketmq.streams.common.cache.compress.CacheKV;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型
 */
public class KeySet extends CacheKV {

    public KeySet(int capacity) {
        super(capacity, false);

    }

    @Override
    public Object get(String key) {
        boolean exist = contains(key);
        if (exist) {
            return key;
        }
        return null;
    }

    @Override
    public void put(String key, Object value) {
        putInner(key, null, true);
    }

    @Override
    public boolean contains(String key) {
        MapElementContext context = queryMapElementByHashCode(key);
        if (context.isMatchKey()) {
            return true;
        }
        return false;
    }

    public void add(String key) {
        super.putInner(key, null, true);
    }

    public static void main(String[] args) throws InterruptedException {
        int count = 10000000;
        KeySet map = new KeySet(count);
        long start = System.currentTimeMillis();
        //    Set<String> values=new HashSet<>();
        //Map<String,Integer> kv=new HashMap<>();
        for (int i = 0; i < count; i++) {
            map.add("sdfsdfdds" + i);
            ;
        }
        System.out.println("cost memory:\t" + map.calMemory() + "M");
        // System.out.println(values.size());
        System.out.println("cost is:\t" + (System.currentTimeMillis() - start));
        start = System.currentTimeMillis();

        for (int i = 0; i < count; i++) {
            boolean v = map.contains("sdfsdfdds" + i);
            if (!v) {
                throw new RuntimeException("");
            }
        }
        System.out.println("cost is:\t" + (System.currentTimeMillis() - start));
    }
}
