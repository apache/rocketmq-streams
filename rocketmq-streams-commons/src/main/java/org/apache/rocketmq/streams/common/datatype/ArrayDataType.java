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
import com.alibaba.fastjson.JSONArray;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class ArrayDataType extends GenericParameterDataType<Object> {

    private static final long serialVersionUID = 1679897087225371547L;
    private DataType paradigmType;
    private transient ListDataType listDataType;

    public ArrayDataType(Class clazz, DataType paradigmType) {
        setDataClazz(clazz);
        this.paradigmType = paradigmType;
        listDataType = new ListDataType(paradigmType);
    }

    public ArrayDataType() {

    }

    @Override
    public String toDataJson(Object objectArray) {
        JSONArray jsonArray = new JSONArray();
        int len = Array.getLength(objectArray);
        for (int i = 0; i < len; i++) {
            Object object = Array.get(objectArray, i);
            if (object == null) {
                jsonArray.add(null);
                continue;
            }
            if (this.dataClazz == null) {
                setDataClazz(objectArray.getClass());
            }
            String data = createStringValue(this.paradigmType, object, this.dataClazz);
            jsonArray.add(data);
        }
        return jsonArray.toJSONString();
    }

    @Override
    public Object getData(String jsonValue) {
        Class clazz = null;
        DataType dataType = this.paradigmType;
        ListDataType listDataType = this.listDataType;
        List list = new ArrayList();
        if (listDataType == null) {
            listDataType = new ListDataType(new StringDataType());
        }
        if (listDataType.isQuickModel(jsonValue)) {
            list = listDataType.getData(jsonValue);
            dataType = new StringDataType();
            clazz = String.class;

        } else {
            JSONArray jsonArray = JSON.parseArray(jsonValue);
            Set<String> classNames = new HashSet<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                String json = jsonArray.getString(i);

                if (json == null) {
                    list.add(null);
                    continue;
                }
                Object result = createObjectValue(this.paradigmType, json, classNames);
                list.add(result);
            }
            String name = classNames.iterator().next();
            clazz = ReflectUtil.forClass(name);
        }

        Object o = Array.newInstance(clazz, list.size());
        for (int i = 0; i < list.size(); i++) {
            Array.set(o, i, list.get(i));
        }
        return o;
    }

    @Override
    public String getName() {
        return "array";
    }

    public static String getTypeName() {
        return "array";
    }

    @Override
    public String getDataTypeName() {
        return "array";
    }

    @Override
    public void parseGenericParameter(String genericParameterString) {
        if (StringUtil.isEmpty(genericParameterString)) {
            return;
        }
        genericParameterString = genericParameterString.trim();
        String className = null;
        if (genericParameterString.endsWith("[]")) {
            className = genericParameterString.substring(0, genericParameterString.length() - 2);
        } else {
            className = genericParameterString.substring(2);
        }
        Class clazz = createClass(className);
        DataType dataType = DataTypeUtil.getDataTypeFromClass(clazz);
        if (GenericParameterDataType.class.isInstance(dataType)) {
            GenericParameterDataType genericParamterDataType = (GenericParameterDataType) dataType;
            genericParamterDataType.parseGenericParameter(className);
            this.paradigmType = dataType;
            return;
        }
        this.paradigmType = dataType;
    }

    @Override
    public String toDataStr(Object ts) {
        ListDataType listDataType = this.listDataType;
        if (listDataType == null) {
            listDataType = new ListDataType(new StringDataType());
        }
        List<Object> list = convert(ts);
        return listDataType.toDataStr(list);
    }

    @Override
    protected String createGenericParameterStr() {
        String subStr = createGenericParameterStr(paradigmType);
        return "[L" + subStr;
    }

    @Override
    public boolean matchClass(Class clazz) {
        return clazz.isArray();
    }

    @Override
    public byte[] toBytes(Object objectArray, boolean isCompress) {
        if (objectArray == null) {
            return null;
        }
        List<byte[]> list = new ArrayList<>();
        int len = 0;
        int arraySize = Array.getLength(objectArray);
        for (int i = 0; i < arraySize; i++) {
            Object o = Array.get(objectArray, i);
            byte[] bytes = createByteValue(this.paradigmType, o);
            list.add(bytes);
            len = len + bytes.length;
        }
        StringDataType stringDataType = new StringDataType();
        byte[] elementClassBytes = stringDataType.toBytes(this.dataClazz.getName(), false);
        len = len + elementClassBytes.length;
        byte[] bytes = new byte[len + 2];
        byte[] lenBytes = createByteArrayFromNumber(arraySize, 2);
        bytes[0] = lenBytes[0];
        bytes[1] = lenBytes[1];
        int i = 2;
        for (byte b : elementClassBytes) {
            bytes[i++] = b;
        }
        for (byte[] bytes1 : list) {
            for (byte b : bytes1) {
                bytes[i++] = b;
            }
        }
        return bytes;
    }

    @Override public void setDataClazz(Class dataClazz) {
        String className = dataClazz.getName();
        String name = className.substring(1);
        name = name.replace(";", "");
        if ("I".equals(name)) {
            this.dataClazz = int.class;
        } else if ("J".equals(name)) {
            this.dataClazz = long.class;
        } else if ("D".equals(name)) {
            this.dataClazz = double.class;
        } else if ("F".equals(name)) {
            this.dataClazz = float.class;
        } else if ("S".equals(name)) {
            this.dataClazz = short.class;
        } else if ("B".equals(name)) {
            this.dataClazz = double.class;
        } else if ("Z".equals(name)) {
            this.dataClazz = boolean.class;
        } else if (name.startsWith("L")) {
            name = name.substring(1);
            this.dataClazz = ReflectUtil.forClass(name);
        } else {
            this.dataClazz = dataClazz;
        }
    }

    @Override
    public Object byteToValue(byte[] bytes) {
        return byteToValue(bytes, 0);
    }

    @Override
    public Object byteToValue(byte[] bytes, AtomicInteger offset) {
        if (bytes == null) {
            return null;
        }
        int len = createNumberValue(bytes, offset.get(), 2).intValue();
        offset.addAndGet(2);
        StringDataType stringDataType = new StringDataType();
        String className = stringDataType.byteToValue(bytes, offset);
        Class clazz = ReflectUtil.forClass(className);
        List list = new ArrayList();
        for (int i = 0; i < len; i++) {
            Object value = createObjectValue(this.paradigmType, bytes, offset);
            list.add(value);
        }

        Object o = Array.newInstance(clazz, len);
        for (int i = 0; i < list.size(); i++) {
            Array.set(o, i, list.get(i));
        }
        return o;
    }

    @Override
    public DataType create() {
        return new ArrayDataType();
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException {
        String name = long.class.getName();
        System.out.println(name);
        Class clazz = ReflectUtil.forClass(name);
//        Float[] strs = new Float[10];
//        ArrayDataType arrayDataType =(ArrayDataType) DataTypeUtil.getDataTypeFromClass(strs.getClass());
    }

    @Override
    public List convert(Object objectArray) {
        if (objectArray == null) {
            return null;
        }
        List tList = new ArrayList<>();
        int len = Array.getLength(objectArray);
        for (int i = 0; i < len; i++) {
            tList.add(Array.get(objectArray, i));
        }
        return tList;
    }

    public void setParadigmType(DataType paradigmType) {
        this.paradigmType = paradigmType;
    }
}
