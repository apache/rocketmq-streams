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
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.datatype.ArrayDataType;
import org.apache.rocketmq.streams.common.datatype.BaseDataType;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.ByteDataType;
import org.apache.rocketmq.streams.common.datatype.ConfigurableDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.DateDataType;
import org.apache.rocketmq.streams.common.datatype.DoubleDataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.GenericParameterDataType;
import org.apache.rocketmq.streams.common.datatype.HllDataType;
import org.apache.rocketmq.streams.common.datatype.IJsonable;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.JavaBeanDataType;
import org.apache.rocketmq.streams.common.datatype.JsonableDataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.datatype.NotSupportDataType;
import org.apache.rocketmq.streams.common.datatype.NumberDataType;
import org.apache.rocketmq.streams.common.datatype.SetDataType;
import org.apache.rocketmq.streams.common.datatype.ShortDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;

@SuppressWarnings("rawtypes")
public class DataTypeUtil {

    private static final Log LOG = LogFactory.getLog(DataTypeUtil.class);

    private static final List<DataType> dataTypes = new ArrayList<>();
    /**
     * 基础类型的名字映射
     */
    private static final Map<String, DataType> baseTypeDataTypeMap = new HashMap<>();
    private static final Map<Class, DataType> class2DataTypeMap = new HashMap<>();
    private static final Map<String, DataType> dbType2DataTypeMap = new HashMap<>();
    private static final Map<Integer, DataType> typeCode2DataType = new HashMap<>();

    static {
        register(new HllDataType());

        register(new NumberDataType());
        register(new StringDataType());
        register(new IntDataType());
        register(new LongDataType());
        register(new BooleanDataType());

        register(new DateDataType());
        register(new DoubleDataType());
        register(new FloatDataType());
        register(new ByteDataType());
        register(new ShortDataType());
        register(new ListDataType());
        register(new SetDataType());
        register(new ArrayDataType());
        register(new MapDataType());
        register(new ConfigurableDataType());
        register(new JsonableDataType());
        register(new JavaBeanDataType());
        register(new NotSupportDataType());
    }

    public static void register(DataType dataType) {
        // 这里要始终确保NotSupportDataType位于最后，因为NotSupportDataType会匹配拦截所有的类型
        //        if (dataType instanceof NotSupportDataType) {
        //            dataTypes.add(dataType);
        //        } else {
        //            dataTypes.add(0, dataType);
        //        }

        dataTypes.add(dataType);
        /*
          对于class是固定的直接放到map中，对于变化的放到dataTypes列表中，可以减少循环次数
         */
        baseTypeDataTypeMap.put(dataType.getDataTypeName(), dataType);
        if (dataType instanceof BaseDataType) {
            BaseDataType baseDataType = (BaseDataType)dataType;
            Class[] classes = baseDataType.getSupportClasses();
            if (classes != null) {
                for (Class c : classes) {
                    class2DataTypeMap.put(c, dataType);
                }
            }
        }

        switch (dataType.getDataTypeName()) {
            case "string": {
                dbType2DataTypeMap.put("VARCHAR", dataType);
                typeCode2DataType.put(Types.VARCHAR, dataType);
            }
            case "int": {
                dbType2DataTypeMap.put("INTEGER", dataType);
                typeCode2DataType.put(Types.INTEGER, dataType);
            }
            case "long": {
                dbType2DataTypeMap.put("BIGINT", dataType);
                typeCode2DataType.put(Types.BIGINT, dataType);
            }
            case "boolean": {
                dbType2DataTypeMap.put("BOOLEAN", dataType);
                typeCode2DataType.put(Types.BOOLEAN, dataType);
            }
            case "Date": {
                dbType2DataTypeMap.put("DATE", dataType);
                typeCode2DataType.put(Types.DATE, dataType);
            }
            case "double": {
                dbType2DataTypeMap.put("DOUBLE", dataType);
                typeCode2DataType.put(Types.DOUBLE, dataType);
            }
            case "float": {
                dbType2DataTypeMap.put("FLOAT", dataType);
                typeCode2DataType.put(Types.FLOAT, dataType);
            }
            case "short": {
                dbType2DataTypeMap.put("SMALLINT", dataType);
                typeCode2DataType.put(Types.SMALLINT, dataType);
            }
            case "array": {
                dbType2DataTypeMap.put("ARRAY", dataType);
            }

        }

    }

    /**
     * 通过class获取datatype
     *
     * @param clazz 类
     * @return DataType
     */
    public static DataType getDataTypeFromClass(Class clazz) {
        DataType type = class2DataTypeMap.get(clazz);
        if (type != null) {
            return type;
        }
        for (DataType dataType : dataTypes) {
            if (dataType.matchClass(clazz)) {
                DataType dataType1 = dataType.create();
                dataType1.setDataClazz(clazz);
                return dataType1;
            }
        }
        return null;
    }

    /**
     * 通过datatype name获取datatype
     *
     * @param dataTypeName dataType 名称
     * @return DataType
     */
    public static DataType getDataType(String dataTypeName) {
        return baseTypeDataTypeMap.get(dataTypeName);
    }

    /**
     * 把序列化后的datatype还原为datatype
     *
     * @param dataTypeJson dataType JSON 信息
     * @return DataType
     */
    public static DataType createDataType(String dataTypeJson) {
        JSONObject jsonObject = JSON.parseObject(dataTypeJson);
        String className = jsonObject.getString(DataType.DATA_TYPE_CLASS_NAME);
        Class clazz = null;
        try {
            clazz = Class.forName(className);
            DataType dataType = getDataTypeFromClass(clazz);
            if (dataType != null) {
                dataType.toObject(dataTypeJson);
            }
            return dataType;
        } catch (ClassNotFoundException e) {
            LOG.error("create dataType error. the detail is " + dataTypeJson);
            throw new RuntimeException("class not find " + className, e);
        }
    }

    public static DataType createDataTypeByDbType(String dbType) {
        return dbType2DataTypeMap.get(dbType);
    }

    public static DataType createDataTypeByDbType(int dbType) {
        return typeCode2DataType.get(dbType);
    }

    /**
     * 根据类和范型的名称创建DataType,
     *
     * @param clazz            类
     * @param genericParameter 范型内容为java.util.Map<String,List<String>,包含类自身的类名
     * @return DataType
     */
    public static DataType createDataType(Class clazz, String genericParameter) {
        DataType dataType = getDataTypeFromClass(clazz);
        if (GenericParameterDataType.class.isInstance(dataType)) {
            GenericParameterDataType genericParamterDataType = (GenericParameterDataType)dataType;
            genericParamterDataType.parseGenericParameter(genericParameter);
        }
        return dataType;
    }

    public static boolean matchClass(Class<?>[] parameterTypes, Class[] classes) {
        if (parameterTypes == null && classes == null) {
            return true;
        }
        if (parameterTypes == null) {
            return false;
        }
        if (classes == null) {
            return false;
        }
        if (parameterTypes.length != classes.length) {
            return false;
        }
        for (int i = 0; i < parameterTypes.length; i++) {
            Class parameterType = parameterTypes[i];
            DataType dataType = DataTypeUtil.getDataTypeFromClass(parameterType);
            if (dataType != null && !dataType.matchClass(classes[i])) {
                return false;
            }
        }
        return true;
    }

    /**
     * 根据类和范型的名称创建DataType,
     *
     * @param genericParameter 泛型参数
     * @param genericParameter 范型内容为java.util.Map<String,List<String>,包含类自身的类名
     * @return DataType
     */
    public static DataType createDataTypeByGenericClass(String genericParameter) {
        try {
            String className = genericParameter;
            int classNameIndex = genericParameter.indexOf("<");
            if (classNameIndex != -1) {
                className = genericParameter.substring(0, classNameIndex);
            }
            Class clazz = Class.forName(className);
            return createDataType(clazz, genericParameter);
        } catch (Exception e) {
            LOG.error("create dataType error. the detail is " + genericParameter);
            throw new RuntimeException("createDataTypeByGenericClass error " + genericParameter);
        }

    }

    /**
     * 根据类和范型的名称创建DataType,
     *
     * @param clazz            类
     * @param genericParameter 范型内容为java.util.Map<String,List<String>,包含类自身的类名
     * @param lazyCheck        当不发现不支持的类型的时候，不直接抛错，在后续使用的时候抛出。在某种场景下创建的类型可能不会被使用
     * @return DataType
     */
    @Deprecated
    public static DataType createDataType(Class clazz, String genericParameter, boolean lazyCheck) {
        DataType dataType = createDataType(clazz, genericParameter);
        if (dataType != null) {
            return dataType;
        }
        if (lazyCheck) {
            return new NotSupportDataType(clazz);
        } else {
            throw new RuntimeException(
                "can not support data type " + clazz + " , genericParameterType:" + genericParameter.toString());
        }

    }

    /**
     * 对一个方法的参数做datatype转化
     *
     * @param method 方法
     * @return DataType
     */
    public static DataType[] createDataType(Method method) {
        Type[] types = method.getGenericParameterTypes();
        Class[] classes = method.getParameterTypes();
        DataType[] dataTypes = new DataType[classes.length];
        for (int i = 0; i < classes.length; i++) {
            String typeString = types[i].toString();
            if (typeString.startsWith("class [L")) {
                typeString = typeString.replace("class [L", "").replace(";", "");
                if (!typeString.endsWith("[]")) {
                    typeString += "[]";
                }
            } else if (typeString.startsWith("class ")) {
                typeString = null;
            }

            DataType dataType = createDataType(classes[i], typeString);
            dataTypes[i] = dataType;
        }
        return dataTypes;

    }

    /**
     * 创建某个对象的字段对应的datatype，字段必须有get方法，否则会抛错
     *
     * @param object    实例
     * @param fieldName 字段名称
     * @return DataType
     */
    public static DataType createFieldDataType(Object object, String fieldName) {
        Class clazz = object.getClass();
        return createFieldDataType(clazz, fieldName);
    }

    /**
     * 创建某个对象的字段对应的datatype，字段必须有get方法，否则会抛错
     *
     * @param clazz     类
     * @param fieldName 字段名称
     * @return DataType
     */
    public static DataType createFieldDataType(Class clazz, String fieldName) {

        Method method = ReflectUtil.getGetMethod(clazz, fieldName);
        Type type = method.getGenericReturnType();
        String typeString = type.toString();
        if (typeString.startsWith("class ")) {
            typeString = null;
        }

        return createDataType(ReflectUtil.getBeanFieldType(clazz, fieldName), typeString);
    }

    public static String convertGenericParameterString(String type) {
        if (type == null) {
            return null;
        }
        String typeString = type.toString();
        if (typeString.startsWith("class ")) {
            typeString = null;
        }
        return typeString;
    }

    public static String convertGenericParameterString(Type type) {
        if (type == null) {
            return null;
        }
        return convertGenericParameterString(type.toString());
    }

    public static boolean isJsonAble(Class clazz) {
        DataType dataType = getDataTypeFromClass(IJsonable.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isNumber(Class clazz) {
        return isInt(clazz) || isLong(clazz) || isDouble(clazz) || isFloat(clazz);
    }

    public static boolean isNumber(DataType dataType) {
        return dataType instanceof DoubleDataType || dataType instanceof IntDataType || dataType instanceof LongDataType || dataType instanceof FloatDataType;
    }

    public static boolean isDate(Class clazz) {
        DataType dataType = getDataTypeFromClass(Date.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isByte(Class clazz) {
        DataType dataType = getDataTypeFromClass(Byte.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isShort(Class clazz) {
        DataType dataType = getDataTypeFromClass(Short.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isTimestamp(Class clazz) {
        DataType dataType = getDataTypeFromClass(Timestamp.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isInt(Class clazz) {
        DataType dataType = getDataTypeFromClass(Integer.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isLong(Class clazz) {
        DataType dataType = getDataTypeFromClass(Long.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isDouble(Class clazz) {
        DataType dataType = getDataTypeFromClass(Double.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isFloat(Class clazz) {
        DataType dataType = getDataTypeFromClass(Float.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isString(Class clazz) {
        DataType dataType = getDataTypeFromClass(String.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isBigDecimal(Class clazz) {
        DataType dataType = getDataTypeFromClass(BigDecimal.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isBoolean(Class clazz) {
        DataType dataType = getDataTypeFromClass(Boolean.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isArray(Class clazz) {
        return clazz.isArray();
    }

    public static boolean isList(Class clazz) {
        DataType dataType = getDataTypeFromClass(List.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isMap(Class clazz) {
        DataType dataType = getDataTypeFromClass(Map.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    public static boolean isJavaBean(Class clazz) {
        DataType dataType = getDataTypeFromClass(clazz);
        return dataType instanceof JavaBeanDataType;
    }

    public static boolean isConfigurable(Class clazz) {
        DataType dataType = getDataTypeFromClass(IConfigurable.class);
        return dataType != null && dataType.matchClass(clazz);
    }

    /**
     * 获取包装类型的基本类型
     *
     * @param o 实例
     * @return 实例
     */
    @Deprecated
    public static Object getBaseType(Object o) {
        if (o == null) {
            return null;
        }
        Class c = o.getClass();
        if (isInt(c)) {
            return o;
        } else if (isLong(c)) {
            return o;
        } else if (isDouble(c)) {
            return o;
        } else if (isFloat(c)) {
            return o;
        } else if (isBoolean(c)) {
            return o;
        } else if (isDate(c)) {
            return o;
        } else if (isString(c)) {
            return o;
        } else if (isBigDecimal(c)) {
            return o;
        } else if (isByte(c)) {
            return o;
        } else if (isShort(c)) {
            return o;
        } else if (isTimestamp(c)) {
            return o;
        }
        return o;
    }

    public static boolean isBoolean(DataType dataType) {
        return dataType instanceof BooleanDataType;
    }

    public static boolean isDate(DataType dataType) {
        return dataType instanceof DateDataType;
    }

    public static void main(String[] args) {
        BooleanDataType booleanDataType = new BooleanDataType();
        boolean result = DataTypeUtil.isDate(booleanDataType);
        System.out.println(Map.class.getSimpleName());
    }

    public static boolean isString(DataType dataType) {
        return dataType instanceof StringDataType;
    }
}
