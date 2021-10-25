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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.compress.ByteArrayValueKV;
import org.apache.rocketmq.streams.common.cache.compress.ICacheKV;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.ByteDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.datatype.DoubleDataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.SetDataType;
import org.apache.rocketmq.streams.common.datatype.ShortDataType;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

/**
 * 常用类型的存储，目前只支持，float，int，long，double，set<String>，date，byte，short,boolean 存入一行数据，且必须是定长的行
 */
public class FixedLenRowCacheKV {
    protected static final int MAX_ELIMENT = 3000000;//最大的元素个数
    protected DataType[] datatypes;
    protected int byteLength = -1;
    protected MultiValueKV<byte[]> cache;//每块存储固定，超过后，用多块存储

    protected Map<Integer, MultiValueKV> keySetMap = new HashMap<>();//存对应的set值。每个KeySet有最大值，超过最大值需要多个对象存储

    public FixedLenRowCacheKV(DataType... datatypes) {
        this.datatypes = datatypes;
        if (datatypes != null) {
            int index = 0;
            int byteLength = 0;
            for (DataType datatype : datatypes) {
                if (datatype instanceof SetDataType) {
                    keySetMap.put(index, new MultiValueKV(MAX_ELIMENT) {
                        @Override
                        protected ICacheKV create() {
                            return new KeySet(this.capacity);
                        }
                    });
                    byteLength++;
                } else if (datatype instanceof FloatDataType || datatype instanceof DoubleDataType || datatype instanceof LongDataType || datatype instanceof DateDataType) {
                    byteLength = byteLength + 8;
                } else if (datatype instanceof IntDataType) {
                    byteLength = byteLength + 4;
                } else if (datatype instanceof ByteDataType || datatype instanceof BooleanDataType) {
                    byteLength++;
                } else if (datatype instanceof ShortDataType) {
                    byteLength = byteLength + 2;
                }
                index++;
            }
        }
        cache = new MultiValueKV<byte[]>(MAX_ELIMENT) {
            @Override
            protected ICacheKV<byte[]> create() {
                return new ByteArrayValueKV(this.capacity, byteLength);
            }
        };
    }

    /**
     * 写入一行数据
     *
     * @param key
     * @param values
     */
    public void put(String key, Object... values) {
        byte[] value = createBytes(values);
        cache.put(key, value);
    }

    /**
     * 获取一行数据
     *
     * @param key
     * @return
     */
    public Object[] get(String key) {
        byte[] value = cache.get(key);
        if (value == null) {
            return null;
        }
        Object[] result = new Object[datatypes.length];
        AtomicInteger offset = new AtomicInteger(0);
        for (int i = 0; i < result.length; i++) {
            DataType dataType = datatypes[i];
            if (dataType instanceof SetDataType) {
                MultiValueKV multiValueKV = keySetMap.get(i);
                result[i] = multiValueKV;
            } else {
                result[i] = dataType.byteToValue(value, offset);
            }

        }
        return result;
    }

    /**
     * 把具体的值，转化成固定长度的byte数组
     *
     * @param values
     * @return
     */
    public byte[] createBytes(Object... values) {
        if (values == null || values.length == 0) {
            return null;
        }
        byte[] bytes = new byte[byteLength];
        int index = 0;
        for (int i = 0; i < values.length; i++) {
            DataType dataType = datatypes[i];
            Object o = values[i];
            byte[] byteValue = null;
            if (dataType instanceof SetDataType) {
                byteValue = new byte[] {(byte) i};
                add2Set(i, (Set<String>) o);
            } else {
                byteValue = dataType.toBytes(o, false);
            }
            NumberUtils.putSubByte2ByteArray(bytes, index, byteValue);
            index = index + byteValue.length;
        }
        return bytes;
    }

    /**
     * 插入数据到set中
     *
     * @param i        第几个变量
     * @param setValue
     */
    protected void add2Set(int i, Set<String> setValue) {
    }

}
