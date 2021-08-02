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

import org.apache.rocketmq.streams.common.cache.compress.impl.IntValueKV;
import org.apache.rocketmq.streams.common.cache.compress.impl.KeySet;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.junit.Test;

public class IntValueKVTest {

    /**
     * 高压缩缓存，只支持固定值存储，目前主要应用IntValueKV，key是任意字符串，value是int值
     */
    @Test
    public void testIntValueKV() {
        int capacity = 1000000;
        IntValueKV cache = new IntValueKV(capacity);
        System.out.println(cache.calMemory());//内存预先分配，100w预先分配22m内存
        for (int i = 0; i < capacity; i++) {
            cache.put("name" + i, i);
        }
        System.out.println(cache.calMemory());//由于存在hash冲突，插入100w数据，内存大概31m
        System.out.println(cache.get("name0") == 0);
    }

    /**
     * 内存会占用更少
     */
    @Test
    public void testKeySet() {
        int capacity = 1000000;
        KeySet cache = new KeySet(capacity);
        System.out.println(cache.calMemory());//内存预先分配，100w预先分配19m内存
        for (int i = 0; i < capacity; i++) {
            cache.add("name" + i);
        }
        System.out.println(cache.calMemory());//由于存在hash冲突，插入100w数据，内存大概27m
        Boolean result = cache.contains("name1") == true;
        System.out.println(result);
    }

    /**
     * 一个int数据，可以按位存储boolean信息，提供了NumberUtils工具类处理数字的bit操作 缓存可以结合NumberUtils，使int存储多个boolean值
     */
    @Test
    public void testNumUtil() {
        int num = 0;
        num = NumberUtils.setNumFromBitMapInt(num, 3);//设置num的第3位为1
        System.out.println(NumberUtils.getNumFromBitMapInt(num, 3) == true);//获取第3位是否为1
    }
}
