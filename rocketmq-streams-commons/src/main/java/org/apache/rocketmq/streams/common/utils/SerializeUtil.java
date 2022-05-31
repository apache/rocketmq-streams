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
package org.apache.rocketmq.streams.common.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.datatype.ArrayDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.interfaces.ISerialize;

public class SerializeUtil {
    private static final Log LOG = LogFactory.getLog(SerializeUtil.class);
   /**
     * 把一个对象序列化成字节,对象中的字段是datatype支持的
     *
     * @param object
     * @return
     */
    public static byte[] serialize(Object object) {
        DataType dataType = DataTypeUtil.getDataTypeFromClass(object.getClass());
        if (ArrayDataType.class.isInstance(dataType)) {
            int length = Array.getLength(object);
            Object[] objects = new Object[length];
            for (int i = 0; i < length; i++) {
                objects[i] = Array.get(object, i);
            }
            object = objects;
        }
        String dataTypeName = dataType.getDataTypeName();
        StringDataType stringDataType = new StringDataType();
        byte[] dataTypeNameBytes = stringDataType.toBytes(dataTypeName, false);
        byte[] values = dataType.toBytes(object, false);
        byte[] result = new byte[dataTypeNameBytes.length + values.length];
        int i = 0;
        for (byte b : dataTypeNameBytes) {
            result[i++] = b;
        }
        for (byte b : values) {
            result[i++] = b;
        }
        return result;
    }

    /**
     * 把一个对象的字段，通过字节填充,字段不能有null值
     *
     * @param bytes
     */
    public static <T> T deserialize(byte[] bytes, AtomicInteger offset) {

        StringDataType stringDataType = new StringDataType();
        String dataTypeName = stringDataType.byteToValue(bytes, offset);
        DataType dataType = DataTypeUtil.getDataType(dataTypeName);
        return (T) dataType.byteToValue(bytes, offset);
    }
    /**
     * 把一个对象的字段，通过字节填充,字段不能有null值
     *
     * @param bytes
     */
    public static <T> T deserialize(byte[] bytes,Class clazz) {
        if(ISerialize.class.isAssignableFrom(clazz)){
            return (T)KryoUtil.readObjectFromByteArray(bytes,clazz);
            //return (T)conf.asObject(bytes);
        }
        return deserialize(bytes, new AtomicInteger(0));
    }
    /**
     * 把一个对象的字段，通过字节填充,字段不能有null值
     *
     * @param bytes
     */
    public static <T> T deserialize(byte[] bytes) {
        T result = null;
        try {
            result = deserialize(bytes, new AtomicInteger(0));
        } catch (NullPointerException npe) {
            if (bytes != null && bytes.length != 0) {
                String temp = new String(bytes, StandardCharsets.UTF_8);
                result = (T) temp;
            }
        }
        return result;
    }

    public static byte[] serializeByJava(Object object) {
        if (!Serializable.class.isInstance(object)) {
            throw new RuntimeException("can not serialize this class " + object.getClass().getName() + ", please implements Serializable");
        }
        try (ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteOutputStream)) {
            objectOutputStream.writeObject(object);
            return byteOutputStream.toByteArray();
        } catch (IOException e) {
            LOG.error("failed in serializing, object = " + Optional.ofNullable(object).orElse("null"), e);
            return new byte[0];
        }
    }

    public static <T> T deserializeByJava(byte[] array) {
        try (ByteArrayInputStream byteInputStream = new ByteArrayInputStream(array);
             ObjectInputStream objectInputStream = new ObjectInputStream(byteInputStream)) {
            return (T) objectInputStream.readObject();
        } catch (Exception e) {
            LOG.error("failed in deserialize, byte array = " + Optional.ofNullable(array).orElse(null), e);
            return null;
        }
    }
}
