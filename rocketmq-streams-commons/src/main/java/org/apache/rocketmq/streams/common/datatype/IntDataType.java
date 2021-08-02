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
package org.apache.rocketmq.streams.common.datatype;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

import java.util.concurrent.atomic.AtomicInteger;

public class IntDataType extends BaseDataType<Integer> {
    private static final long serialVersionUID = -7780238614760001804L;

    public IntDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public IntDataType() {
        setDataClazz(Integer.class);
    }

    @Override
    public Integer getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        return Integer.valueOf(jsonValue);
    }

    @Override
    public String getName() {
        return int.class.getSimpleName();
    }

    public static String getTypeName() {
        return "int";
    }

    @Override
    public String toDataJson(Integer value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public int convertValue(Integer value) {
        if (value == null) {
            return 0;
        }
        return value.intValue();
    }

    @Override
    public boolean matchClass(Class clazz) {
        return Integer.class.isAssignableFrom(clazz) || int.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Integer.class, int.class};
    }

    @Override
    public DataType create() {
        return new IntDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public byte[] toBytes(Integer value, boolean isCompress) {
        if (isCompress) {
            return numberToBytes((long)value);
        } else {
            return createByteArrayFromNumber(value, 4);
        }
    }

    @Override
    public Integer byteToValue(byte[] bytes) {
        Long value = createNumberValue(bytes);
        if (value == null) {
            return null;
        }
        return value.intValue();
    }

    @Override
    public Integer byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 4);
        offset.set(index + 4);
        return byteToValue(bytesArray);
    }
}
