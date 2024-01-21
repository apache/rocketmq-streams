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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseDataType<T> implements DataType<T>, Serializable {

    protected final static String CODE = "UTF-8";
    private static final long serialVersionUID = 987223978957770805L;
    private static final Logger LOG = LoggerFactory.getLogger(BaseDataType.class);

    /**
     * 数据类型
     */
    protected Class dataClazz;

    @Override
    public Class getDataClass() {
        return this.dataClazz;
    }

    @Override
    public String toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(DATA_TYPE_CLASS_NAME, this.dataClazz.getName());
        setFieldValueToJson(jsonObject);
        return jsonObject.toJSONString();
    }

    @Override
    public String getName() {
        return this.getClass().getSimpleName();
    }

    protected abstract void setFieldValueToJson(JSONObject jsonObject);

    protected abstract void setFieldValueFromJson(JSONObject jsonObject);

    @Override
    public void toObject(String jsonString) {
        // 子类需要时做扩展，主要是把新增的成员变量做序列化。在DataUtil中，可以根据clazz和genericParameterString创建对应的DataType
        JSONObject jsonObject = JSON.parseObject(jsonString);
        String className = jsonObject.getString(DATA_TYPE_CLASS_NAME);
        try {

            Class clazz = Class.forName(className.trim());
            this.dataClazz = clazz;
            setFieldValueFromJson(jsonObject);
        } catch (Exception e) {
            throw new RuntimeException("class not find " + className, e);
        }

    }

    public Class getDataClazz() {
        return dataClazz;
    }

    @Override
    public void setDataClazz(Class dataClazz) {
        this.dataClazz = dataClazz;
    }

    protected Class createClass(String className) {
        try {
            return Class.forName(className.trim());
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException("class not found " + className, e);
        }
    }

    @Override
    public T convert(Object object) {
        return (T) object;
    }

    @Override
    public byte[] toBytes(T value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        String str = toDataJson(value);
        try {
            byte[] result = str.getBytes(CODE);

            byte[] bytes = new byte[result.length + 2];
            byte[] lenBytes = createByteArrayFromNumber(result.length, 2);
            bytes[0] = lenBytes[0];
            bytes[1] = lenBytes[1];
            for (int i = 2; i < bytes.length; i++) {
                bytes[i] = result[i - 2];
            }
            return bytes;
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("can not support encoding exception-UTF8", e);
        }
    }

    @Override
    public T byteToValue(byte[] bytes) {
        try {
            if (bytes == null || bytes.length == 0) {
                return null;
            }
            byte[] valueBytes = new byte[bytes.length - 2];
            for (int i = 0; i < valueBytes.length; i++) {
                valueBytes[i] = bytes[i + 2];
            }

            String value = new String(valueBytes, CODE).trim();
            if (value == null) {
                return null;
            }
            return this.getData(value);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("can not support encoding exception-UTF8", e);
        }
    }

    @Override
    public T byteToValue(byte[] bytes, int index) {
        AtomicInteger offset = new AtomicInteger(index);
        return byteToValue(bytes, offset);
    }

    @Override
    public T byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        int len = createNumberValue(bytes, index, 2).intValue();
        byte[] values = NumberUtils.getSubByteFromIndex(bytes, index, len + 2);
        offset.set(index + len + 2);
        return byteToValue(values);
    }

    protected byte[] numberToBytes(Long number) {
        if (number == null) {
            return null;
        }
        if (number >= getMinValue(-7) && number <= (getMaxValue(7))) {
            return new byte[] {number.byteValue()};
        } else if (number >= getMinValue(-15) && number <= getMaxValue(15)) {
            return createByteArrayFromNumber(number, 2);
        } else if (number >= getMinValue(-23) && number <= getMaxValue(23)) {
            return createByteArrayFromNumber(number, 3);
        } else if (number >= getMinValue(-31) && number <= getMaxValue(31)) {
            return createByteArrayFromNumber(number, 4);
        } else if (number >= getMinValue(-39) && number <= getMaxValue(39)) {
            return createByteArrayFromNumber(number, 5);
        } else if (number >= getMinValue(-47) && number <= getMaxValue(47)) {
            return createByteArrayFromNumber(number, 6);
        } else if (number >= getMinValue(-55) && number <= getMaxValue(55)) {
            return createByteArrayFromNumber(number, 7);
        } else if (number >= getMinValue(-63) && number <= getMaxValue(63)) {
            return createByteArrayFromNumber(number, 8);
        }
        return null;
    }

    protected byte[] createByteArrayFromNumber(long value, int byteCount) {
        byte[] byteRet = new byte[byteCount];
        for (int i = 0; i < byteCount; i++) {
            byteRet[i] = (byte) ((value >> 8 * i) & 0xff);
        }
        return byteRet;
    }

    protected Long createNumberValue(byte[] arr) {
        return createNumberValue(arr, 0, arr.length);
    }

    protected Long createNumberValue(byte[] arr, int offset, int size) {
        if (arr == null) {
            return null;
        }
        long value = 0;
        for (int i = offset; i < offset + size; i++) {
            value |= ((long) (arr[i] & 0xff)) << (8 * (i - offset));
        }
        return value;
    }

    protected long getMinValue(int byteLength) {
        return (long) Math.pow(2, byteLength);
    }

    protected long getMaxValue(int byteLength) {
        long value = (long) Math.pow(2, byteLength);
        return (value - 1);
    }

    /**
     * 能够支持的class,如int,Integer等。对于class不确定的返回空。主要是为了做优化，快速获取类型使用
     *
     * @return
     */
    protected Class[] getSupportClass() {
        return null;
    }

    public Class[] getSupportClasses() {
        return getSupportClass();
    }
}
