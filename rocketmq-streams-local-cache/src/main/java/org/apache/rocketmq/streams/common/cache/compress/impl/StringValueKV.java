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

import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.streams.common.cache.compress.ByteArrayValueKV;
import org.apache.rocketmq.streams.common.cache.compress.ICacheKV;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型 只能一次行load，不能进行更新
 */
public class StringValueKV implements ICacheKV<String> {

    protected final static String CODE = "UTF-8";
    protected ByteArrayValueKV values;

    public StringValueKV(int capacity) {
        values = new ByteArrayValueKV(capacity);
    }

    @Override
    public String get(String key) {
        byte[] bytes = values.get(key);
        if (bytes == null) {
            return null;
        }
        try {
            return new String(bytes, CODE);
        } catch (Exception e) {
            throw new RuntimeException("can not convert byte 2 string ", e);
        }
    }

    @Override
    public void put(String key, String value) {

        try {
            byte[] bytes = value.getBytes(CODE);
            values.put(key, bytes);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("can not convert byte 2 string ", e);
        }

    }

    @Override
    public boolean contains(String key) {
        return values.contains(key);
    }

    @Override
    public int getSize() {
        return values.getSize();
    }

    @Override
    public int calMemory() {
        return values.calMemory();
    }

}
