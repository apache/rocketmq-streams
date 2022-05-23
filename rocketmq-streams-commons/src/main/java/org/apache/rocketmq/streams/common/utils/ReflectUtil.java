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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.configurable.annotation.NoSerialized;
import org.apache.rocketmq.streams.common.datatype.ArrayDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.NotSupportDataType;
import org.apache.rocketmq.streams.common.datatype.SetDataType;

/**
 * 类ReflectUtil的实现描述
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class ReflectUtil {
    private static final String AUTOWIRED_CLASS_NAME = "org.springframework.beans.factory.annotation.Autowired";
    private static Class AUTOWIRED_CLASS = null;
    private static final Log LOG = LogFactory.getLog(ReflectUtil.class);
    public static final String SPLIT_SIGN = "\\.";                               // 及联调用的分隔符。如a.b.c

    static {
        try {
            AUTOWIRED_CLASS = Class.forName(AUTOWIRED_CLASS_NAME);
        } catch (Exception e) {
            AUTOWIRED_CLASS = null;
        }
    }

    public static <T> T getDeclaredField(Class clazz, Object object, String fieldName) {
        try {
            Method method = getGetMethod(clazz, fieldName);
            if (method != null) {
                return (T) method.invoke(object);
            }
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return (T) field.get(object);
        } catch (Exception e) {
            String msg = fieldName;
            if (object != null) {
                msg = object.getClass().getName() + ":" + fieldName;
            }
            throw new RuntimeException("get field value error " + msg, e);
        }
    }

    public static <T> T getDeclaredField(Object object, String fieldName) {
        return getDeclaredField(object.getClass(), object, fieldName);
    }

    public static List<Field> getDeclaredFieldsContainsParentClass(Class c) {
        List<Field> fieldList = new ArrayList<>();
        Class current = c;
        while (current != null) {
            Field[] fields = current.getDeclaredFields();
            for (Field field : fields) {
                fieldList.add(field);
            }
            current = current.getSuperclass();
        }
        return fieldList;
    }

    /**
     * 对于无参的构造函数创建实例，如果有参数需要提前set，则提供参数名称：value的映射关系
     *
     * @param clazz
     * @param parametes
     * @return
     */
    public static <T> T createObject(Class<T> clazz, String... parametes) {
        try {
            Object object = clazz.newInstance();
            if (parametes != null) {
                for (String kv : parametes) {
                    String[] values = kv.split(":");
                    String propertyName = values[0];
                    Object value = values[1];
                    if (values.length > 2) {
                        String type = values[1];
                        DataType dataType = DataTypeUtil.getDataType(type);
                        value = dataType.getData(values[2]);
                    }
                    setBeanFieldValue(object, propertyName, value);
                }
            }
            return (T) object;
        } catch (Exception e) {
            throw new RuntimeException("can not create object " + clazz.getName());
        }
    }

    public static Class forClass(String className) {
        try {
            if ("int".equals(className)) {
                return int.class;
            } else if ("long".equals(className)) {
                return long.class;
            } else if ("short".equals(className)) {
                return short.class;
            } else if ("double".equals(className)) {
                return double.class;
            } else if ("float".equals(className)) {
                return float.class;
            } else if ("boolean".equals(className)) {
                return boolean.class;
            }
            Class clazz = Class.forName(className);
            return clazz;
        } catch (Exception e) {
            throw new RuntimeException("create class error " + className, e);
        }
    }

    public static <T> T forInstance(Class clazz) {
        try {
            return (T) clazz.newInstance();
        } catch (Exception e) {
            throw new RuntimeException("create instance error " + clazz.getName(), e);
        }
    }

    public static <T> T forInstance(String className) {
        Class c = forClass(className);
        return forInstance(c);

    }

    public static Object deserializeObject(JSONObject jsonObject) {
        String className = jsonObject.getString(IConfigurableService.CLASS_NAME);
        if (StringUtil.isEmpty(className)) {
            throw new RuntimeException("can not Deserialize object ，the class name is empty");
        }
        Object object = ReflectUtil.forInstance(className);
        scanFields(object, (o, field) -> {
            String fileName = field.getName();
            Class fieldClass=ReflectUtil.getBeanFieldType(o.getClass(),fileName);
            DataType dataType = DataTypeUtil.getDataTypeFromClass(fieldClass);
            String genericTypeStr = null;
            if (field.getGenericType() != null) {
                genericTypeStr = field.getGenericType().toString();
            }
            if (NotSupportDataType.class.isInstance(dataType)) {
                Object object1 = ReflectUtil.getDeclaredField(o, field.getName());
                if (object1 != null) {
                    //如果是接口类，则通过值获取类型
                    dataType = DataTypeUtil.getDataTypeFromClass(object1.getClass());
                    if (genericTypeStr != null) {
                        int startIndex = genericTypeStr.indexOf("<");
                        if (startIndex > -1) {
                            genericTypeStr = dataType.getDataClass().getName() + genericTypeStr.substring(startIndex);
                        }
                    }

                }
            }
            setDataTypeParadigmType(dataType, genericTypeStr, ParameterizedType.class.isInstance(field.getGenericType()));
            String fieldJson = jsonObject.getString(fileName);
            if (fieldJson == null) {
                return;
            }
            Object value = dataType.getData(fieldJson);
            try {
                if (value != null) {
                    ReflectUtil.setBeanFieldValue(object,field.getName(),value);
//                    field.setAccessible(true);
//                    field.set(object, value);
                }
            } catch (Exception e) {
                throw new RuntimeException("Deserialize error ,the field " + fileName + " Deserialize error ");
            }
        });
        return object;
    }

    /**
     * 把一个对象序列化成json
     *
     * @param o
     * @return
     */
    public static JSONObject serializeObject(Object o) {
        if (o == null) {
            return null;
        }

        JSONObject objectJson = new JSONObject();
        objectJson.put(IConfigurableService.CLASS_NAME, o.getClass().getName());
        scanFields(o, (o1, field) -> {
            field.setAccessible(true);
            String fileName = field.getName();
            Class fieldClass=ReflectUtil.getBeanFieldType(o1.getClass(),fileName);
            DataType dataType = DataTypeUtil.getDataTypeFromClass(fieldClass);
            String genericTypeStr = null;
            if (field.getGenericType() != null) {
                genericTypeStr = field.getGenericType().toString();
            }
            if (NotSupportDataType.class.isInstance(dataType)) {
                Object object1 = ReflectUtil.getDeclaredField(o, field.getName());
                if (object1 != null) {
                    //如果是接口类，则通过值获取类型
                    dataType = DataTypeUtil.getDataTypeFromClass(object1.getClass());
                    if (genericTypeStr != null) {
                        int startIndex = genericTypeStr.indexOf("<");
                        if (startIndex > -1) {
                            genericTypeStr = dataType.getDataClass().getName() + genericTypeStr.substring(startIndex);
                        }
                    }

                }
            }
            setDataTypeParadigmType(dataType, genericTypeStr, ParameterizedType.class.isInstance(field.getGenericType()));
            try {
                Object value = ReflectUtil.getDeclaredField(o1,fileName);
                if (value == null) {
                    return;
                }
                objectJson.put(fileName, dataType.toDataJson(value));
            } catch (Exception e) {
                throw new RuntimeException("serializeObject error ,the field " + fileName + " serialize error ");
            }

        });
        return objectJson;
    }

    public static byte[] serialize(Object object) {
        return SerializeUtil.serializeByJava(object);
    }

    public static Object deserialize(byte[] array) {
        return SerializeUtil.deserializeByJava(array);
    }

    public static void scanConfiguableFields(Object o, IFieldProcessor fieldProcessor) {
        scanConfiguableFields(o, o.getClass(), fieldProcessor);
    }

    public static void scanConfiguableFields(Object o, Class clazz, IFieldProcessor fieldProcessor) {
        scanFields(o, clazz, fieldProcessor, AbstractConfigurable.class);
    }

    public static void scanFields(Object o, IFieldProcessor fieldProcessor) {
        scanFields(o, o.getClass(), fieldProcessor, Object.class);
    }

    /**
     * 把一个对象的字段，通过字节填充,字段不能有null值
     *
     * @param object
     * @param bytes
     */
    public static void deserializeObject(Object object, byte[] bytes) {
        AtomicInteger offset = new AtomicInteger(0);
        ReflectUtil.scanFields(object, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                field.setAccessible(true);
                Class fieldClass=ReflectUtil.getBeanFieldType(o.getClass(),field.getName());
                DataType dataType = DataTypeUtil.getDataTypeFromClass(fieldClass);
                String genericTypeStr = null;
                if (field.getGenericType() != null) {
                    genericTypeStr = field.getGenericType().toString();
                }
                if (NotSupportDataType.class.isInstance(dataType)) {
                    Object object1 = ReflectUtil.getDeclaredField(o, field.getName());
                    if (object1 != null) {
                        //如果是接口类，则通过值获取类型
                        dataType = DataTypeUtil.getDataTypeFromClass(object1.getClass());
                        if (genericTypeStr != null) {
                            int startIndex = genericTypeStr.indexOf("<");
                            if (startIndex > -1) {
                                genericTypeStr = dataType.getDataClass().getName() + genericTypeStr.substring(startIndex);
                            }
                        }

                    }
                }
                setDataTypeParadigmType(dataType, genericTypeStr, ParameterizedType.class.isInstance(field.getGenericType()));
                Object value = dataType.byteToValue(bytes, offset.get());
                offset.addAndGet(dataType.toBytes(value, false).length);
                try {
                    ReflectUtil.setBeanFieldValue(object,field.getName(),value);
                } catch (Exception e) {
                    throw new RuntimeException("can not set field value " + field.getName(), e);
                }
            }
        });
    }

    /**
     * 把一个对象序列化成字节,字段不能有null值
     *
     * @param object
     * @return
     */
    public static byte[] serializeObject2Byte(Object object) {
        List<byte[]> list = new ArrayList<>();
        AtomicInteger byteCount = new AtomicInteger(0);
        AtomicInteger fieldIndex = new AtomicInteger(0);
        AtomicInteger fieldCount = new AtomicInteger(0);
        ReflectUtil.scanFields(object, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                field.setAccessible(true);
                Class clazz = field.getType();
                DataType dataType = DataTypeUtil.getDataTypeFromClass(clazz);
                String genericTypeStr = null;
                if (field.getGenericType() != null) {
                    genericTypeStr = field.getGenericType().toString();
                }
                if (NotSupportDataType.class.isInstance(dataType)) {
                    Object object1 = ReflectUtil.getDeclaredField(o, field.getName());
                    if (object1 != null) {
                        //如果是接口类，则通过值获取类型
                        dataType = DataTypeUtil.getDataTypeFromClass(object1.getClass());
                        if (genericTypeStr != null) {
                            int startIndex = genericTypeStr.indexOf("<");
                            if (startIndex > -1) {
                                genericTypeStr = dataType.getDataClass().getName() + genericTypeStr.substring(startIndex);
                            }
                        }

                    }
                }
                setDataTypeParadigmType(dataType, genericTypeStr, ParameterizedType.class.isInstance(field.getGenericType()));

                byte[] bytes = new byte[0];
                try {
                    Object value = field.get(object);
                    if (value == null) {

                    }
                    bytes = dataType.toBytes(value, false);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("can not get field value " + field.getName(), e);
                }
                if (bytes == null) {
                    bytes = new byte[0];
                }
                list.add(bytes);
                byteCount.addAndGet(bytes.length);
            }
        });
        byte[] bytes = new byte[byteCount.get()];
        int i = 0;
        for (byte[] bytes1 : list) {
            for (byte b : bytes1) {
                bytes[i] = b;
                i++;
            }
        }
        return bytes;
    }

    /**
     * 如果是集合类型，设置对应的paradigm type
     *
     * @param dataType
     * @param genericType
     */
    public static void setDataTypeParadigmType(DataType dataType, String genericType, boolean isGenericType) {
        if (ListDataType.class.isInstance(dataType)) {
            String genericTypeStr = "java.util.List<java.lang.String>";
            if (genericType != null && ParameterizedType.class.isInstance(genericType)) {
                genericTypeStr = DataTypeUtil.convertGenericParameterString(genericType);
            }
            ((ListDataType) dataType).parseGenericParameter(genericTypeStr);
        } else if (SetDataType.class.isInstance(dataType)) {
            String genericTypeStr = "java.util.Set<java.lang.String>";
            if (genericType != null && isGenericType) {
                genericTypeStr = DataTypeUtil.convertGenericParameterString(genericType);
            }
            ((SetDataType) dataType).parseGenericParameter(genericTypeStr);
        } else if (MapDataType.class.isInstance(dataType)) {
            String genericTypeStr = "java.util.Map<java.lang.String,java.lang.String>";
            if (genericType != null && isGenericType) {
                genericTypeStr = DataTypeUtil.convertGenericParameterString(genericType);
            }
            ((MapDataType) dataType).parseGenericParameter(genericTypeStr);
        } else if (ArrayDataType.class.isInstance(dataType)) {
            String genericTypeStr = "java.lang.String[]";
            if (genericType != null && isGenericType) {
                genericTypeStr = DataTypeUtil.convertGenericParameterString(genericType);
            }
            ((ArrayDataType) dataType).parseGenericParameter(genericTypeStr);
        }
    }

    /**
     * @param o
     * @param clazz
     * @param fieldProcessor
     */
    public static void scanFields(Object o, Class clazz, IFieldProcessor fieldProcessor, Class basedClass) {
        if (basedClass.getName().equals(clazz.getName())) {
            return;
        }
        Field[] fields = clazz.getDeclaredFields();
        if (fields == null) {
            return;
        }

        for (Field field : fields) {
            try {
                if (field.isAnnotationPresent(NoSerialized.class)) {
                    continue;
                }
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                } else if (Modifier.isTransient(field.getModifiers())) {
                    continue;
                } else if (Modifier.isNative(field.getModifiers())) {
                    continue;
                }
                fieldProcessor.doProcess(o, field);
            }catch (Exception e){
                throw new RuntimeException("Error serializing object, class is "+clazz.getName()+", field is "+field.getName(),e);
            }

        }
        Class parent = clazz.getSuperclass();
        scanFields(o, parent, fieldProcessor, basedClass);
    }

    public static void setFieldValue2Object(Object o, JSONObject jsonObject) {
        if (jsonObject == null || o == null) {
            LOG.warn("setFieldValue2Object o or jsonObject is null ");
            return;
        }
        setFieldValue2Object(o, o.getClass(), jsonObject);
    }

    protected static void setFieldValue2Object(Object o, Class clazz, JSONObject jsonObject) {
        if (BasedConfigurable.class.getName().equals(clazz.getName())) {
            return;
        }
        if (Object.class.getName().equals(clazz.getName())) {
            return;
        }
        Field[] fields = clazz.getDeclaredFields();
        if (fields == null) {
            return;
        }
        for (Field field : fields) {
            if (field.isAnnotationPresent(NoSerialized.class)) {
                continue;
            }
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            } else if (Modifier.isTransient(field.getModifiers())) {
                continue;
            } else if (Modifier.isNative(field.getModifiers())) {
                continue;
            }
            DataType dataType = DataTypeUtil.createFieldDataType(o, field.getName());
            String fieldJsonStr = jsonObject.getString(field.getName());
            Object fieldValue = dataType.getData(fieldJsonStr);
            if (fieldValue != null) {
                ReflectUtil.setBeanFieldValue(o, field.getName(), fieldValue);
            }
        }
        Class parent = clazz.getSuperclass();
        setFieldValue2Object(o, parent, jsonObject);
    }

    /**
     * 执行某个字段的get方法
     *
     * @param fieldName 字段名称
     * @param obj       字段所属对象
     * @return 执行结果
     */
    public static Object invokeGetMethod(String fieldName, Object obj) {
        try {
            Method method = getGetMethod(obj.getClass(), fieldName);
            return method.invoke(obj);
        } catch (Exception e) {
            throw new RuntimeException("invokeGetMethod error， the fieldName is " + fieldName + "; the object is " + obj, e);
        }
    }

    /**
     * 反射执行一个对象的方法
     *
     * @param object
     * @param methodName
     * @param classes
     * @param objects
     * @return
     */
    public static Object invoke(Object object, String methodName, Class[] classes, Object[] objects) {
        try {
            Class clazz = object.getClass();
            Method method = clazz.getMethod(methodName, classes);

            return method.invoke(object, objects);
        } catch (Exception e) {
            throw new RuntimeException("invokeGetMinvokeethod error， the methodName is " + methodName + "; the object is " + object, e);
        }
    }

    /**
     * 获取一个javabean的field值,支持及联查询，如a.b.c的方式，这里通过get或is方法获取。如果不存在这些方法将会获取失败
     *
     * @param bean      符合java bean规范，有get，set方法
     * @param fieldName 字段名称
     * @param <T>       返回结果类型
     * @return 返回结果
     */
    public static <T> T getBeanFieldOrJsonValue(Object bean, String fieldName) {
        String[] fieldNames = fieldName.split(SPLIT_SIGN);
        Object result = null;
        Object modelBean = bean;// ChannelMessage
        for (String name : fieldNames) {// messageBody.type
            if (modelBean == null) {
                return null;
            }
            if (JSONObject.class.isInstance(modelBean)) {
                result = getJsonItemValue(modelBean, name);
            } else if (JSONArray.class.isInstance(modelBean)) {
                result = getJsonItemValue(modelBean, name);
            } else if (Map.class.isInstance(modelBean)) {
                result = ((Map) modelBean).get(name);
            } else if(String.class.isInstance(modelBean)){
                String value=(String)modelBean;
                if (value.startsWith("[") && value.endsWith("]")) {
                    modelBean = JSON.parseArray(value);
                } else {
                    modelBean = JSON.parseObject(value);
                }
                result=getJsonItemValue(modelBean,name);
            }else {
                result = getDeclaredField(modelBean, name);// ChannelMessage.messageBody
            }
            modelBean = result;
        }
        return (T) result;
    }

    private static <T> T getJsonItemValue(Object bean, String fieldName) {
        if (fieldName.indexOf("[") == -1 && JSONObject.class.isInstance(bean)) {
            JSONObject jsonObject = (JSONObject) bean;
            Object value = jsonObject.get(fieldName);
            return (T) value;
        }
        if (fieldName.indexOf("[") != -1) {
            if (JSONObject.class.isInstance(bean)) {
                JSONObject jsonObject = (JSONObject) bean;
                int startIndex = fieldName.indexOf("[");
                String word = fieldName.substring(0, startIndex);
                JSONArray jsonArray = jsonObject.getJSONArray(word);
                String lastStr = fieldName.substring(startIndex);
                return getArrayIndexValue(jsonArray, lastStr);
            } else if (JSONArray.class.isInstance(bean)) {
                JSONArray jsonArray = (JSONArray) bean;
                int startIndex = fieldName.indexOf("[");
                String lastStr = fieldName.substring(startIndex);
                return getArrayIndexValue(jsonArray, lastStr);
            }
            return null;
        }
        return null;
    }

    private static <T> T getArrayIndexValue(JSONArray jsonArray, String indexDescription) {
        int startIndex = indexDescription.indexOf("[");
        int endIndex = indexDescription.indexOf("]");
        while (startIndex < endIndex) {
            String indexStr = indexDescription.substring(startIndex + 1, endIndex);
            if (indexStr.equals("*")) {
                return (T) jsonArray.toJSONString();
            }
            int index = Integer.valueOf(indexStr);
            indexDescription = indexDescription.substring(endIndex + 1);
            startIndex = indexDescription.indexOf("[");
            if (startIndex == -1) {
                return (T) jsonArray.get(index);
            } else {
                jsonArray = jsonArray.getJSONArray(index);
            }
            endIndex = indexDescription.indexOf("]");
        }
        return null;
    }

    /**
     * 获取一个javabean的field值,支持及联查询，如a.b.c的方式，这里通过get或is方法获取。如果不存在这些方法将会获取失败
     *
     * @param bean      符合java bean规范，有get，set方法
     * @param fieldName 字段名称
     * @param <T>       返回结果类型
     * @return 返回结果
     */
    public static <T> T getBeanFieldOrJsonValue(Object bean, Class clazz, String fieldName) {
        return getBeanFieldOrJsonValue(bean, fieldName);
    }

    /**
     * 获取一个javabean的field值,支持及联查询，如a.b.c的方式，这里通过get或is方法获取。如果不存在这些方法将会获取失败
     *
     * @param bean      符合java bean规范，有get，set方法
     * @param fieldName 字段名称
     * @param <T>       返回结果类型
     * @return 返回结果
     */
    public static <T> T getBeanFieldValue(Object bean, String fieldName) {
        String[] fieldNames = fieldName.split(SPLIT_SIGN);
        Object result = null;
        Object modelBean = bean;
        for (String name : fieldNames) {
            result = getFieldValue(modelBean, name);
            modelBean = result;
        }
        return (T) result;

    }

    public static void setFieldValue(Object object, String fieldName, Object fieldValue) {
        try {
            if (fieldValue == null) {
                return;
            }
            Class clazz = object.getClass();

            Field field = findField(clazz, fieldName);
            field.setAccessible(true);
            field.set(object, fieldValue);
        } catch (Exception e) {
            throw new RuntimeException("set field value error " + object.getClass().getName() + "." + fieldName, e);
        }
    }

    public static Field findField(Class clazz, String fieldName) {
        if (clazz == null) {
            return null;
        }
        Field[] fields = clazz.getDeclaredFields();
        Field field = null;
        for (Field subField : fields) {
            if (subField.getName().equals(fieldName)) {
                field = subField;
                break;
            }
        }
        if (field != null) {
            return field;
        }
        return findField(clazz.getSuperclass(), fieldName);
    }

    /**
     * 跟javabean设置值，必须有set方法
     *
     * @param object
     * @param modelFieldName 字段名
     * @param fieldValue     字段值
     */
    public static void setBeanFieldValue(Object object, String modelFieldName, Object fieldValue) {
        Method method = getSetMethod(object.getClass(), modelFieldName, fieldValue.getClass());
        try {

            DataType dataType = DataTypeUtil.getDataTypeFromClass(fieldValue.getClass());
            Object convertFieldValue = dataType.convert(fieldValue);
            if (method != null) {
                method.setAccessible(true);
                method.invoke(object, convertFieldValue);
            } else {
                Field field = object.getClass().getDeclaredField(modelFieldName);
                field.setAccessible(true);
                field.set(object, fieldValue);
            }

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("setBeanFieldValue error， the modelFieldName is " + modelFieldName + "; the object is " + object + "; the fieldValue is " + fieldValue + ", the field value class is " + fieldValue.getClass().getName(), e);
        }
    }

    /**
     * 获取javabean的字段类型，要求必须有get方法
     *
     * @param clazz
     * @param modelFieldName 字段名称
     * @return 字段的类型
     */
    public static Class getBeanFieldType(Class clazz, String modelFieldName) {
        String[] fieldNames = modelFieldName.split(SPLIT_SIGN);
        Class aClass = clazz;
        Class result = null;
        for (String fieldName : fieldNames) {
            Method method = getGetMethod(aClass, fieldName);
            if (method == null) {
                return null;
            }
            result = getFieldType(aClass, fieldName);
            aClass = result;
        }
        return result;
    }

    public static boolean isJavaBean(Class clazz) {
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            if (AUTOWIRED_CLASS != null && field.getAnnotation(AUTOWIRED_CLASS) != null) {
                continue;
            }
            int mod = field.getModifiers();
            if (Modifier.isFinal(mod) || Modifier.isNative(mod) || Modifier.isStatic(mod) || Modifier.isStrict(mod) || Modifier.isTransient(mod)) {
                continue;
            }

            String fieldName = field.getName();
            Method getMethod = ReflectUtil.getGetMethod(clazz, fieldName);
            Method setMethod = ReflectUtil.getSetMethod(clazz, fieldName, field.getType());
            if (getMethod == null || setMethod == null) {
                return false;
            }
        }
        return true;
    }

    /**
     * 获取一个javabean的field值，这里通过get或is方法获取。如果不存在这些方法将会获取失败
     *
     * @param bean      符合java bean规范，有get，set方法
     * @param fieldName 字段名称
     * @param <T>
     * @return
     */
    private static <T> T getFieldValue(Object bean, String fieldName) {
        try {
            if (bean == null) {
                return null;
            }
            Class clazz = bean.getClass();
            Method method = getGetMethod(clazz, fieldName);
            if (method == null) {
                throw new RuntimeException("can not get " + fieldName + "'s value, the method is not exist");
            }
            method.setAccessible(true);
            return (T) method.invoke(bean);
        } catch (Exception e) {
            throw new RuntimeException("can not get " + fieldName + "'s value", e);
        }
    }

    private static Class getFieldType(Class clazz, String modelFieldName) {
        Method method = getGetMethod(clazz, modelFieldName);
        if (method == null) {
            return null;
        }
        return method.getReturnType();
    }

    public static Method getSetMethod(Class<?> aClass, String modelFieldName, Class<?> aClass1) {
        String setMethodName = getSetMethodName(modelFieldName);
        if (!existMethodName(aClass, setMethodName)) {
            setMethodName = getIsMethodSetName(aClass, setMethodName, modelFieldName);
        }
        try {
            aClass1 = DataTypeUtil.getDataTypeFromClass(aClass1).getDataClass();
            Method method = findMethodFromClass(aClass, setMethodName, new Class[] {aClass1});

            return method;
        } catch (Exception e) {
            LOG.error(aClass.getSimpleName() + "\r\n" + e.getMessage(), e);
            throw new RuntimeException(aClass.getSimpleName() + "\r\n" + e.getMessage() + " ,modelFieldName: " + modelFieldName, e);
        }
    }

    private static Method findMethodFromClass(Class<?> aClass, String methodName, Class[] classes) {
        Method[] methods = aClass.getMethods();
        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                if (DataTypeUtil.matchClass(method.getParameterTypes(), classes)) {
                    return method;
                }
            }
        }
        return null;
    }

    public static Method getGetMethod(Class clazz, String fieldName) {
        try {
            Method method = null;
            String methodName = getGetMethodName(fieldName);
            methodName = getIsMethodGetName(clazz, methodName, fieldName);
            if (methodName == null) {
                return null;
            }
            method = clazz.getMethod(methodName);
            return method;
        } catch (Exception e) {
            LOG.error(clazz.getName() + "\r\n" + e.getMessage() + " ,fieldName: " + fieldName + "\r\n" + e.getMessage(), e);
            throw new RuntimeException(clazz.getName() + "\r\n" + e.getMessage() + " ,fieldName: " + fieldName, e);
        }

    }

    private static String getIsMethodSetName(Class clazz, String methodName, String fieldName) {
        if (!existMethodName(clazz, methodName)) {
            methodName = getIsSetMethodName(fieldName);
        }
        if (existMethodName(clazz, methodName)) {
            return methodName;
        }
        if (fieldName.startsWith("is")) {
            return fieldName.replace("is", "set");
        }
        return null;
    }

    private static String getIsMethodGetName(Class clazz, String methodName, String fieldName) {
        if (!existMethodName(clazz, methodName)) {
            methodName = getIsMethodName(fieldName);
        } else {
            return methodName;
        }
        if (existMethodName(clazz, methodName)) {
            return methodName;
        }

        if (fieldName.startsWith("is")) {
            methodName = fieldName.replace("is", "get");
        }
        if (existMethodName(clazz, methodName)) {
            return methodName;
        }
        return null;
    }

    /**
     * 判断一个类是否存在某个名字的方法
     *
     * @param clazz
     * @param methodName
     * @return
     */
    public static Boolean existMethodName(Class clazz, String methodName) {
        Method method = getMethodByName(clazz, methodName);
        if (method != null) {
            return true;
        }
        return false;
    }

    /**
     * 判断一个类是否存在某个名字的方法
     *
     * @param clazz
     * @param methodName
     * @return
     */
    public static Method getMethodByName(Class clazz, String methodName) {
        Method[] methods = clazz.getMethods();
        for (Method method : methods) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        return null;
    }

    /**
     * 获取get方法
     *
     * @param fieldName
     * @return
     */
    private static String getGetMethodName(String fieldName) {
        String methodName = "get" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return methodName;
    }

    /**
     * 如果是boolean类型，有可能生成的是is方法
     *
     * @param fieldName
     * @return
     */
    private static String getIsMethodName(String fieldName) {
        if (fieldName.startsWith("is")) {
            return fieldName;
        }
        String methodName = "is" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return methodName;
    }

    private static String getSetMethodName(String fieldName) {
        String setMethodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return setMethodName;
    }

    /**
     * 如果是boolean类型，有可能生成的是is方法
     *
     * @param fieldName
     * @return
     */
    private static String getIsSetMethodName(String fieldName) {
        if (fieldName.startsWith("is")) {
            fieldName = fieldName.substring(2);
        }
        String methodName = "set" + fieldName.substring(0, 1).toUpperCase() + fieldName.substring(1);
        return methodName;
    }

    public static Object forInstance(Class clazz, Class[] classes, Object[] objects) {
        try {
            Constructor constructor = clazz.getConstructor(classes);
            return constructor.newInstance(objects);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

}
