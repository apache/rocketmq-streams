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

public class ByteDataType extends BaseDataType<Byte> {
    private static final long serialVersionUID = -2840915715701908090L;

    public ByteDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public ByteDataType() {

        setDataClazz(Byte.class);
    }

    @Override
    public String toDataJson(Byte value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    @Override
    public Byte getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        return Byte.valueOf(jsonValue);
    }

    public static String getTypeName() {
        return "byte";
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public boolean matchClass(Class clazz) {
        return Byte.class.isAssignableFrom(clazz) | byte.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Byte.class, byte.class};
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
    public byte[] toBytes(Byte value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        return new byte[] {value};
    }

    @Override
    public Byte byteToValue(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return bytes[0];
    }

    @Override
    public Byte byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        offset.set(index + 1);
        return bytes[index];
    }
}
