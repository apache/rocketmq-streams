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
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

import java.util.List;

/**
 * 支持key是string，value是int的场景，支持size不大于10000000.只支持int，long，boolean，string类型
 */
public class ListValueKV<T> extends CacheKV<List<T>> {

    protected StringValueKV stringValueKV;
    protected DataType<T> datatype;
    protected ListDataType listDataType;
    protected transient Class elementClass;

    public ListValueKV(int capacity, Class elementClass) {
        super(0);
        stringValueKV = new StringValueKV(capacity, false);
        datatype = DataTypeUtil.getDataTypeFromClass(elementClass);
        listDataType = new ListDataType(datatype);
        this.elementClass = elementClass;
    }

    @Override
    public List<T> get(String key) {
        String value = stringValueKV.get(key);
        if (value == null) {
            return null;
        }

        return listDataType.getData(value);
    }

    @Override
    public void put(String key, List<T> values) {
        String value = listDataType.toDataJson(values);
        ;
        stringValueKV.put(key, value);
    }

    @Override
    public boolean contains(String key) {
        return stringValueKV.contains(key);
    }

}
