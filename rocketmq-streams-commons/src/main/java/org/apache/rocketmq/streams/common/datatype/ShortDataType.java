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

public class ShortDataType extends BaseDataType<Short> {
    /**
     *
     */
    private static final long serialVersionUID = -4298519654546507041L;

    public ShortDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public ShortDataType() {
        setDataClazz(Short.class);
    }

    @Override
    public String toDataJson(Short value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    @Override
    public Short getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        return Short.valueOf(jsonValue);
    }

    @Override
    public boolean matchClass(Class clazz) {
        return Short.class.isAssignableFrom(clazz) || short.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Short.class, short.class};
    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public String getName() {
        return Short.class.getSimpleName();
    }

    public static String getTypeName() {
        return "short";
    }

    @Override
    public byte[] toBytes(Short value, boolean isCompress) {
        if (isCompress) {
            return numberToBytes((long)value);
        } else {
            return createByteArrayFromNumber(value, 2);
        }
    }

    @Override
    public Short byteToValue(byte[] bytes) {
        Long value = createNumberValue(bytes);
        if (value == null) {
            return null;
        }
        return value.shortValue();
    }

    @Override
    public Short byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 2);
        offset.set(index + 2);
        return byteToValue(bytesArray);
    }
}
