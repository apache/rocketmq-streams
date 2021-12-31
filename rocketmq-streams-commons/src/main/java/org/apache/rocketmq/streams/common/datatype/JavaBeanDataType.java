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
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class JavaBeanDataType extends BaseDataType {

    protected static ICache<String, AtomicBoolean> cache = new SoftReferenceCache<>();

    private static final long serialVersionUID = 8749848859175999778L;

    public JavaBeanDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public JavaBeanDataType() {

    }

    @Override
    public String toDataJson(Object value) {
        if (value == null) {
            return null;
        }
        byte[] bytes = serializeJavabean(value);
        return Base64Utils.encode(bytes);
    }

    @Override
    public Object getData(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        byte[] bytes = Base64Utils.decode(jsonValue);
        return deserializeJavaBean(bytes);
    }

    @Override
    public String getName() {
        return "javaBean";
    }

    public static String getTypeName() {
        return "javaBean";
    }

    @Override
    public boolean matchClass(Class clazz) {

        AtomicBoolean atomicBoolean = cache.get(clazz.getName());
        if (atomicBoolean != null) {
            return atomicBoolean.get();
        }
        atomicBoolean = new AtomicBoolean(true);

        AtomicBoolean success = atomicBoolean;

        ReflectUtil.scanFields(clazz, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                field.setAccessible(true);
                DataType dataType = DataTypeUtil.createDataTypeByField(o, field);

                if (dataType == null) {
                    success.set(false);
                }
                if (!success.get()) {
                    return;
                }
            }
        });
        cache.put(clazz.getName(), success);
        return success.get();
    }

    @Override
    public DataType create() {
        return new JavaBeanDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override public byte[] toBytes(Object value, boolean isCompress) {
        if (value == null) {
            return null;
        }

        byte[] result = serializeJavabean(value);

        byte[] bytes = new byte[result.length + 2];
        byte[] lenBytes = createByteArrayFromNumber(result.length, 2);
        bytes[0] = lenBytes[0];
        bytes[1] = lenBytes[1];
        for (int i = 2; i < bytes.length; i++) {
            bytes[i] = result[i - 2];
        }
        return bytes;
    }

    @Override public Object byteToValue(byte[] bytes) {
        byte[] valueBytes = new byte[bytes.length - 2];
        for (int i = 0; i < valueBytes.length; i++) {
            valueBytes[i] = bytes[i + 2];
        }
        return deserializeJavaBean(valueBytes);
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {dataClazz};
    }

    protected static ICache<String, ClassMeta> metaMap = new SoftReferenceCache<>();

    /**
     * 把一个对象序列化成字节,对象中的字段是datatype支持的
     *
     * @param object
     * @return
     */
    private static byte[] serializeJavabean(Object object) {
        List<byte[]> list = new ArrayList<>();
        AtomicInteger byteCount = new AtomicInteger(0);
        AtomicInteger fieldIndex = new AtomicInteger(0);
        AtomicBoolean success = new AtomicBoolean(true);
        List<Integer> nullFieldIndex = new ArrayList<>();

        ClassMeta meta = metaMap.get(object.getClass().getName());
        if (meta == null) {
            meta = new ClassMeta();
            metaMap.put(object.getClass().getName(), meta);
        }
        final ClassMeta classMeta = meta;
        ReflectUtil.scanFields(object, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                field.setAccessible(true);

                DataType dataType = classMeta.dataTypeMap.get(field.getName());
                if (dataType == null) {
                    dataType = DataTypeUtil.createDataTypeByField(o, field);
                    classMeta.dataTypeMap.put(field.getName(), dataType);
                }

                if (dataType == null) {
                    success.set(false);
                }
                if (!success.get()) {
                    fieldIndex.incrementAndGet();
                    return;
                }
                Object value = null;
                Boolean useGetMethod = classMeta.useGetMethod.get(field.getName());
                Method method = null;
                if (useGetMethod == null) {
                    method = ReflectUtil.getGetMethod(object.getClass(), field.getName());
                    if (method == null || method.getReturnType().equals(field.getType())) {
                        useGetMethod = false;
                    } else {
                        useGetMethod = true;
                    }
                    classMeta.useGetMethod.put(field.getName(), useGetMethod);
                }
                if (!useGetMethod) {
                    try {
                        value = field.get(object);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                } else {
                    value = ReflectUtil.getDeclaredField(object, field.getName());
                }

                if (value == null) {
                    nullFieldIndex.add(fieldIndex.get());
                    fieldIndex.incrementAndGet();
                    return;
                }

                byte[] bytes = dataType.toBytes(value, false);
                fieldIndex.incrementAndGet();
                list.add(bytes);
                byteCount.addAndGet(bytes.length);

            }
        });

        if (!success.get()) {
            return null;
        }

        DataType classDataType = DataTypeUtil.getDataTypeFromClass(String.class);
        byte[] classNameBytes = (classDataType.toBytes(object.getClass().getName(), false));

        //null field bit
        int columSize = fieldIndex.get() + 1;
        BitSetCache.BitSet bitSet = new BitSetCache.BitSet(columSize);
        for (Integer index : nullFieldIndex) {
            bitSet.set(index);
        }
        byte[] nullIndexBytes = bitSet.getBytes();

        //bit set length
        byte bitsetLengthBytes = NumberUtils.toByte(nullIndexBytes.length)[0];
        byte[] bytes = new byte[byteCount.get() + 1 + nullIndexBytes.length + classNameBytes.length];

        int i = 0;

        for (byte b : classNameBytes) {
            bytes[i++] = b;
        }

        bytes[i++] = bitsetLengthBytes;

        for (byte b : nullIndexBytes) {
            bytes[i++] = b;
        }

        for (byte[] bytes1 : list) {
            for (byte b : bytes1) {
                bytes[i] = b;
                i++;
            }
        }
        return bytes;
    }

    /**
     * 把一个对象的字段，通过字节填充,字段不能有null值
     *
     * @param bytes
     */
    private static <T> T deserializeJavaBean(byte[] bytes) {

        DataType classDataType = DataTypeUtil.getDataTypeFromClass(String.class);
        AtomicInteger index = new AtomicInteger(0);
        String className = (String) classDataType.byteToValue(bytes, index);
        Class clazz = null;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            return null;
        }

        Object object = ReflectUtil.forInstance(clazz);

        if (object == null) {
            return null;
        }

        ByteDataType byteDataType = new ByteDataType();
        byte b = byteDataType.byteToValue(bytes, index);
        int len = NumberUtils.toInt(b);
        byte[] bitsetBytes = new byte[len];
        for (int i = 0; i < bitsetBytes.length; i++) {
            bitsetBytes[i] = byteDataType.byteToValue(bytes, index);
        }

        BitSetCache.BitSet bitSet = new BitSetCache.BitSet(bitsetBytes);

        AtomicInteger columnIndex = new AtomicInteger(0);
        ClassMeta classMeta = metaMap.get(className);
        ReflectUtil.scanFields(object, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                field.setAccessible(true);
                DataType dataType = null;
                if (classMeta != null) {
                    dataType = classMeta.dataTypeMap.get(field.getName());
                }
                if (dataType == null) {
                    dataType = DataTypeUtil.createDataTypeByField(o, field);
                }
                if (bitSet.get(columnIndex.get())) {
                    columnIndex.incrementAndGet();
                    return;
                }
                Object value = dataType.byteToValue(bytes, index);

                Boolean useGetMethod = null;
                if (classMeta != null && classMeta.useGetMethod != null) {
                    useGetMethod = classMeta.useGetMethod.get(field.getName());
                }
                if (useGetMethod != null && !useGetMethod) {
                    try {
                        field.set(object, value);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                } else {
                    ReflectUtil.setBeanFieldValue(object, field.getName(), value);
                }

                columnIndex.incrementAndGet();
            }
        });
        return (T) object;
    }

    protected static class ClassMeta {
        Map<String, DataType> dataTypeMap = new HashMap<>();
        Map<String, Boolean> useGetMethod = new HashMap<>();
    }
}
