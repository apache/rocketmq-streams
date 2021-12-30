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
package org.apache.rocketmq.streams.script.function.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.NotSupportDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 每一个注册的function会有一个engine来代表。引擎主要用于执行一个service的方法
 */
public class FunctionConfigure {

    private static final Log LOG = LogFactory.getLog(FunctionConfigure.class);

    /**
     * 函数名称，可以把任务springbean的方法注册成function，并取一个functionname
     */
    public static final String FUNCTION_NAME = "functionName";

    /**
     * 通过这个值从data json中获取需要执行方法的实际参数数据
     */
    public static final String FUNCTION_PARAMETERS = "paramters";

    /**
     * 要执行的方法
     */
    private Method method;

    /**
     * 参数类型列表
     */
    private DataType[] parameterDataTypes;

    private Object bean;

    private FunctionType functionType = FunctionType.UDF;

    private DataType returnDataType;

    private boolean isUserDefinedUDTF = false;//是否用户用规范自定义的udtf

    /**
     * 主要做性能优化的参数组合，可以更快的组装成反射信息
     */
    private List<Integer> noStringDataTypeIndex = new ArrayList<>();

    /**
     * 为了做优化，是否所有的参数都是字符串，这种就不需要验证了
     */
    private boolean isAllParaString = false;

    /**
     * 是否是IMessage和context开头的，如果是值为true
     */
    private boolean startWithContext = false;

    /**
     * 是否是变参数，最后一个参数是数组
     */
    private boolean isVariableParameter = false;

    public FunctionConfigure(String functionName, Method method, Object bean) {
        this.method = method;
        parameterDataTypes = DataTypeUtil.createDataType(method);

        this.bean = bean;
        Class clazz = method.getReturnType();
        this.returnDataType = DataTypeUtil.getDataTypeFromClass(clazz);

        /**
         * 把不是string的参数，记录下来，只对这部分参数做类型转换
         */
        int index = 0;
        for (DataType dataType : parameterDataTypes) {
            if (!dataType.getDataTypeName().equals(StringDataType.getTypeName())) {
                noStringDataTypeIndex.add(index);
            }
            index++;
        }

        /**
         * 打标识是否前缀是imessage，abstractcontext
         */
        if (parameterDataTypes.length > 1 && parameterDataTypes[0].getDataClass().isAssignableFrom(IMessage.class) && parameterDataTypes[1].getDataClass().isAssignableFrom(AbstractContext.class)) {
            startWithContext = true;
        }

        /**
         *除了环境参数，其他参数是否全部为字符串
         */
        int i = 0;
        if (startWithContext) {
            i = 2;
        }
        boolean isString = true;
        for (; i < parameterDataTypes.length; i++) {
            if (!parameterDataTypes[i].getDataTypeName().equals(StringDataType.getTypeName())) {
                isString = false;
                break;
            }
        }
        isAllParaString = isString;

        /**
         * 最后一个参数是否为可变参数
         */
        Class[] classes = method.getParameterTypes();
        if (classes.length > 0 && classes[classes.length - 1].isArray()) {
            isVariableParameter = true;//是否是变参数
        }
    }

    public FunctionConfigure(String functionName, Method method, Object bean, FunctionType functionType) {
        this(functionName, method, bean);
        if (functionType != null) {
            this.functionType = functionType;
        }
    }

    public FunctionType getFunctionType() {
        return functionType;
    }

    public boolean isUDF() {
        return functionType.name().equals(FunctionType.UDF.name());
    }

    public boolean isUDAF() {
        return functionType.name().equals(FunctionType.UDAF.name());
    }

    public boolean isUDTF() {
        return functionType.name().equals(FunctionType.UDTF.name());
    }

    public DataType[] getParameterDataTypes() {
        return parameterDataTypes;
    }

    /**
     * 执行一个函数方法。
     *
     * @param bean           函数方法所属的bean，方法的参数构成＝数据参数＋配置参数
     * @param jsonConfigure  配置参数，在参数列表的后部分
     * @param dataParameters 数据参数，在参数列表的前部分
     * @return 执行结果
     */
    public Object execute(Object bean, String jsonConfigure, Object... dataParameters) {
        return execute(bean, jsonConfigure, false, dataParameters);
    }

    public Object directReflectExecute(Object... dataParameters) {
        return execute(bean, null, true, dataParameters);
    }

    /**
     * 执行一个函数方法。
     *
     * @param bean           函数方法所属的bean，方法的参数构成＝数据参数＋配置参数
     * @param jsonConfigure  配置参数，在参数列表的后部分
     * @param dataParameters 数据参数，在参数列表的前部分
     * @return 执行结果
     */
    public Object execute(Object bean, String jsonConfigure, boolean needDirectReflect, Object... dataParameters) {
        try {
            Object[] parameters = dataParameters;
            if (!needDirectReflect) {
                parameters = getRealParameters(jsonConfigure, dataParameters);
            }
            method.setAccessible(true);
            return method.invoke(bean, parameters);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("执行方法出错" + method.getName(), e);
        }
    }

    /**
     * 如果参数存在变参数，需要做特殊处理 根据参数和方法签名，做方法匹配。目前看，这块会存在问题，如果重载的方法，是全部是string，而传入的参数不是，可能会有问题
     *
     * @param parameters
     * @return
     */
    public Object[] getRealParameters(Object[] parameters) {

        /**
         * 如果方法带imessage和context，而参数不带
         * 或参数头两个参数是imessag和content，方法不是，则返回null，认为不匹配
         */
        if (isStartWithContext()) {
            if (parameters.length < 2) {
                return null;
            }
            if (parameters[0] == null || parameters[1] == null) {
                return null;
            }
            if (!(parameters[0] instanceof IMessage) || !(parameters[1] instanceof AbstractContext)) {
                return null;
            }
        } else {
            if (parameters.length >= 2 && parameters[0] instanceof IMessage && AbstractContext.class.isInstance(parameters[1])) {
                return null;
            }
        }

        //如果不是变参数
        if (!isVariableParameter) {
            if (parameterDataTypes.length != parameters.length) {
                return null;
            }
            //如果全是字符串，不需要做参数转换
            if (isAllParaString()) {
                if (parameters.length == 0) {
                    return parameters;
                }
                int i = 0;
                if (isStartWithContext()) {
                    i = 2;
                }
                for (; i < parameters.length; i++) {
                    DataType<?> dataType = parameterDataTypes[i];
                    Object value = parameters[i];
                    Object realValue = getRealValue(dataType, value);
                    if (value != null && realValue == null) {
                        return null;
                    }
                    parameters[i] = realValue;
                }
                return parameters;
            }
            for (Integer index : noStringDataTypeIndex) {
                DataType<?> dataType = parameterDataTypes[index];
                Object value = parameters[index];
                Object realValue = getRealValue(dataType, value);
                if (value != null && realValue == null) {
                    return null;
                }
                parameters[index] = realValue;
            }
            return parameters;
        }

        Class[] classes = method.getParameterTypes();
        Object[] realParameter = new Object[classes.length];
        if (isVariableParameter && (parameters[parameters.length - 1] == null || !parameters[parameters.length - 1].getClass().isArray())) {
            /**
             * 特殊处理，自建函数都是带message，context前缀的。如果udf是一个数组，可能会被认为是符合的。所以要做下特殊判断
             */
            if (parameterDataTypes.length == 1 && parameters.length >= 2 && parameters[0] instanceof IMessage && parameters[1] instanceof AbstractContext) {
                return null;
            }
            for (int i = 0; i < (classes.length - 1); i++) {
                DataType<?> dataType = parameterDataTypes[i];
                Object value = parameters[i];
                Object realValue = getRealValue(dataType, value);
                if (value != null && realValue == null) {
                    return null;
                }
                realParameter[i] = realValue;

            }
            int classLength = classes.length - 1;
            Class elementClass = classes[classLength].getComponentType();
            DataType dataType = DataTypeUtil.getDataTypeFromClass(elementClass);
            Object lastObjectArray = Array.newInstance(classes[classLength].getComponentType(), parameters.length - (classLength));

            for (int i = 0; i < (parameters.length - (classLength)); i++) {
                Object value = parameters[(classLength) + i];
                Object realValue = value;
                if (value != null && !elementClass.isInstance(value)) {
                    realValue = dataType.getData(value.toString());
                }
                Array.set(lastObjectArray, i, realValue);
            }
            realParameter[classLength] = lastObjectArray;
            return realParameter;
        }
        return parameters;
    }

    public Object getRealValue(int parameterIndex, Object value) {

        DataType<?> datatype = this.parameterDataTypes[parameterIndex];
        return getRealValue(datatype, value);

    }

    private Object getRealValue(DataType<?> dataType, Object value) {
        try {
            if (value == null) {
                return null;
            }
            if (DataTypeUtil.isNumber(dataType) || DataTypeUtil.isBoolean(dataType) || DataTypeUtil.isDate(dataType)) {

                value = dataType.getData(value.toString());
                if (value == null) {
                    return null;//说明参数不匹配，直接返回
                }
            } else if (NotSupportDataType.class.isInstance(dataType)) {
                if (dataType.getDataClass().isAssignableFrom(value.getClass())) {
                    return value;
                }
                return null;
            } else {
                if (DataTypeUtil.isString(dataType) == false && String.class.isInstance(value)) {
                    value = dataType.getData(value.toString());
                }
                if (DataTypeUtil.isString(dataType) && !String.class.isInstance(value)) {
                    value = value.toString();
                }
            }
        } catch (Exception e) {
            return null;//说明参数不匹配，直接返回
        }
        return value;
    }

    public Object[] getRealParameters(String jsonConfigure, Object... dataParamters) {
        Object[] parameters = parseParameters(jsonConfigure, dataParamters);
        if (parameters != null && parameters.length > 0) {
            return getRealParameters(parameters);
        } else {
            return new Object[0];
        }
    }

    /**
     * 验证配置参数和函数结构是否一致
     *
     * @param jsonConfigure
     * @return
     */
    public boolean validateConfigure(String jsonConfigure) {
        JSONObject jsonObject = JSONObject.parseObject(jsonConfigure);
        String jsonArrayForString = jsonObject.getString(FunctionConfigure.FUNCTION_PARAMETERS);
        JSONArray configureParamterlist = JSON.parseArray(jsonArrayForString);
        int configureParameterIndex = getParameterDataTypes().length - configureParamterlist.size();
        Object[] parameters = parseParameters(jsonConfigure, configureParameterIndex);
        if (parameters == null) {
            return false;
        }
        return true;
    }

    /**
     * 从参数数据中解析出参数
     *
     * @param parameterConfigure 参数数据，对应handleconfigure中的configure
     * @return
     */
    private Object[] parseParameters(String parameterConfigure, Object... dataParamters) {
        try {
            if (dataParamters == null) {
                dataParamters = new Object[0];
            }
            if (StringUtil.isEmpty(parameterConfigure)) {
                return dataParamters;
            }
            if (parameterDataTypes.length == dataParamters.length) {
                return dataParamters;
            }
            Object[] parameters = fillDataParameters(dataParamters);
            int configureParameterIndex = dataParamters.length;
            Object[] configureParameters = parseParameters(parameterConfigure, configureParameterIndex, parameterDataTypes.length);
            int i = 0;
            for (Object object : configureParameters) {
                parameters[i + configureParameterIndex] = object;
                i++;
            }

            return parameters;
        } catch (Exception e) {
            LOG.error("parseParameters error :" + parameterConfigure + ";detail info is " + e.getMessage(), e);
            return null;
        }

    }

    /**
     * 从参数数据中解析出参数
     *
     * @param parameterConfigure 参数数据，对应handleconfigure中的configure
     * @return
     */
    public Object[] parseParameters(String parameterConfigure, int configureParameterIndex, int allParamsSize) {
        try {

            int nextParameterIndex = configureParameterIndex;
            JSONObject jsonObject = JSONObject.parseObject(parameterConfigure);
            String jsonArrayForString = jsonObject.getString(FunctionConfigure.FUNCTION_PARAMETERS);
            //JSONArray configureParamterlist = JSON.parseArray(jsonArrayForString);
            Map<String, String> configureParamterMap = JSONObject.parseObject(jsonArrayForString, new TypeReference<Map<String, String>>() {
            });
            Object[] parameters = new Object[allParamsSize - configureParameterIndex];
            for (int i = nextParameterIndex; i < allParamsSize; i++) {
                String value = configureParamterMap.get(Integer.toString(i));
                if (StringUtil.isNotEmpty(value)) {
                    DataType dataType = this.parameterDataTypes[i];
                    parameters[i - nextParameterIndex] = dataType.getData(value);
                }
            }
            return parameters;
        } catch (Exception e) {
            LOG.error("parseParameters error :" + parameterConfigure + ";detail info is " + e.getMessage(), e);
            return null;
        }

    }

    /**
     * 给function的配置参数赋值。不需要些数据参数部分，写上后会被自动忽略
     *
     * @param functionName 函数名称
     * @param objects      配置参数数组对象
     * @return jsong string
     */
    public String createFunctionJson(String functionName, Object... objects) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(FUNCTION_NAME, functionName);
        if (objects == null) {
            return jsonObject.toJSONString();
        }
        JSONArray jsonArray = new JSONArray();
        int i = 0;
        int length = this.parameterDataTypes.length;
        for (Object object : objects) {
            jsonArray.add(parameterDataTypes[length - objects.length + i].toDataJson(object));
            i++;
        }
        jsonObject.put(FUNCTION_PARAMETERS, jsonArray);
        return jsonObject.toJSONString();
    }

    public Object getBean() {
        return this.bean;
    }

    /**
     * 获得方法名
     *
     * @param jsonConfigure
     * @return
     */
    public static String parseFunctionName(String jsonConfigure) {
        JSONObject jsonObject = JSONObject.parseObject(jsonConfigure);
        return jsonObject.getString(FUNCTION_NAME);
    }

    /**
     * 初始化参数列表，先放入数据参数，数据参数在列表的最前面
     *
     * @param headParamters
     * @return
     */
    private Object[] fillDataParameters(Object[] headParamters) {
        Object[] parseParameters = new Object[parameterDataTypes.length];
        for (int i = 0; i < headParamters.length; i++) {
            parseParameters[i] = headParamters[i];
        }
        return parseParameters;
    }

    public Method getMethod() {
        return method;
    }

    public Object execute(Object[] parameters) {
        return execute(this.getBean(), null, parameters);
    }

    public boolean matchMethod(Object... parameters) {
        if ((CollectionUtil.isEmpty(parameters)) && CollectionUtil.isEmpty(parameterDataTypes)) {
            return true;
        }
        parameters = getRealParameters(parameters);
        if (parameters == null) {
            return false;
        }
        if (parameters.length != this.parameterDataTypes.length) {
            return false;
        }

        //        for (int i = 0; i < parameterDataTypes.length; i++) {
        //            DataType dataType = this.parameterDataTypes[i];
        //            Object value = parameters[i];
        //            if(value==null){
        //                continue;
        //            }
        //            if (!dataType.matchClass(value.getClass())) {
        //                return false;
        //            }
        //        }
        return true;

    }

    public boolean startWith(Class[] classes) {
        try {
            for (int i = 0; i < classes.length; i++) {
                DataType<?> dataType = this.parameterDataTypes[i];
                if (!dataType.getDataClass().isAssignableFrom(classes[i]) && !classes[i].isAssignableFrom(dataType.getDataClass())) {
                    return false;
                }
            }
        } catch (Exception e) {
            LOG.error("startWith执行异常，将返回true", e);
        }
        return true;

    }

    public DataType getReturnDataType() {
        return returnDataType;
    }

    public void setReturnDataType(DataType returnDataType) {
        this.returnDataType = returnDataType;
    }

    public boolean isAllParaString() {
        return isAllParaString;
    }

    public boolean isStartWithContext() {
        return startWithContext;
    }

    public boolean isVariableParameter() {
        return isVariableParameter;
    }

    public boolean isUserDefinedUDTF() {
        return isUserDefinedUDTF;
    }

    public void setUserDefinedUDTF(boolean userDefinedUDTF) {
        isUserDefinedUDTF = userDefinedUDTF;
    }
}
