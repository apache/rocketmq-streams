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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class BooleanDataType extends BaseDataType<Boolean> {
    /**
     *
     */
    private static final long serialVersionUID = 2342650922304281979L;

    public BooleanDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public BooleanDataType() {
        setDataClazz(Boolean.class);
    }

    public static String getTypeName() {
        return boolean.class.getSimpleName();
    }

    @Override
    public String toDataJson(Boolean value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    @Override
    public Boolean getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        if ("true".equals(jsonValue.toLowerCase())) {
            return true;
        }
        if ("false".equals(jsonValue.toLowerCase())) {
            return false;
        }
        return null;
    }

    public boolean convertValue(Boolean value) {
        if (value == null) {
            return false;
        }
        return value.booleanValue();
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public Boolean convert(Object object) {
        if (object == null) {
            return false;
        }
        if (Boolean.class.isInstance(object)) {
            return (Boolean) object;
        }
        if (String.class.isInstance(object)) {
            return Boolean.valueOf((String) object);
        }
        Integer value = Integer.valueOf(object.toString());
        return value == 1 ? true : false;
    }

    @Override
    public boolean matchClass(Class clazz) {
        return boolean.class.isAssignableFrom(clazz) | Boolean.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Boolean.class, boolean.class};
    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    public byte[] toBytes(Boolean value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        byte bytes = value ? (byte) 1 : (byte) 0;
        return createByteArrayFromNumber(bytes, 1);
    }

    @Override
    public Boolean byteToValue(byte[] bytes) {
        Long value = createNumberValue(bytes);
        if (value == null) {
            return null;
        }
        return value == 1 ? true : false;
    }

    @Override
    public Boolean byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 1);
        offset.set(index + 1);
        return byteToValue(bytesArray);
    }

}
