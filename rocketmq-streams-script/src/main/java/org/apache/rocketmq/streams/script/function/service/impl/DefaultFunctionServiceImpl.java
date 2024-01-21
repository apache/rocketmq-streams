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
package org.apache.rocketmq.streams.script.function.service.impl;

import com.alibaba.fastjson.JSONObject;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigureMap;
import org.apache.rocketmq.streams.script.function.model.FunctionInfoMap;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.function.service.IDipperInterfaceAdpater;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultFunctionServiceImpl implements IFunctionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultFunctionServiceImpl.class);

    /**
     * 重要，函数注册中心。FunctionConfigureMap 保存多个同名的FunctionConfigure
     */
    protected Map<String, FunctionConfigureMap> functionName2Engies = new IngoreInsensitiveMap<FunctionConfigureMap>();

    /**
     * udaf使用该逻辑。可以注册一个接口
     */
    protected Map<String, Object> className2InnerInterface = new HashMap<String, Object>();

    /**
     * 给URLPythonScript使用
     */
    @Deprecated
    protected Map<String, IDipperInterfaceAdpater> functionName2Adapter = new IngoreInsensitiveMap<>();

    /**
     * 保存参数和描述信息，这个只是展示过程中使用，无业务逻辑
     */
    @Deprecated
    protected Map<String, FunctionInfoMap> functionParam2Engies = new IngoreInsensitiveMap<FunctionInfoMap>();

    public void registeFunction(String functionName, IDipperInterfaceAdpater dipperInterfaceAdpater) {
        this.functionName2Adapter.put(functionName, dipperInterfaceAdpater);
    }

    @Override
    public void registeUserDefinedUDTFFunction(String functionName, Object bean, Method method) {
        FunctionConfigure functionConfigure = registeFunctionInner(functionName, bean, method, FunctionType.UDTF);
        functionConfigure.setUserDefinedUDTF(true);
    }

    @Override
    public void registeFunction(String functionName, Object bean, Method method, FunctionType functionType) {
        registeFunctionInner(functionName, bean, method, functionType);
    }

    private FunctionConfigure registeFunctionInner(String functionName, Object bean, Method method, FunctionType functionType) {
        // LOG.debug("FunctionServiceImpl registeFunction,functionName:" + functionName + " ,method:" + method);
        try {
            if (IDipperInterfaceAdpater.class.isInstance(bean)) {
                registeFunction(functionName, (IDipperInterfaceAdpater) bean);
                return null;
            }
            FunctionConfigureMap functionConfigureMap = functionName2Engies.get(functionName);
            if (functionConfigureMap == null) {
                functionConfigureMap = new FunctionConfigureMap();
                functionName2Engies.put(functionName, functionConfigureMap);
            }
            FunctionConfigure engine = new FunctionConfigure(functionName, method, bean, functionType);
            functionConfigureMap.registFunction(engine);
            return engine;
        } catch (Exception e) {
            LOGGER.error("DefaultFunctionServiceImpl registeFunction error", e);
            throw new RuntimeException("can not regeiste this method " + functionName);
        }
    }

    /**
     * 注册一个函数
     *
     * @param functionName 函数名称
     * @param method       bean中的方法
     * @return 一个执行引擎
     */
    @Override
    public void registeFunction(String functionName, Object bean, Method method) {
        registeFunction(functionName, bean, method, FunctionType.UDF);
    }

    /**
     * 注册一个接口，并给接口取一个名字
     *
     * @param name
     * @param interfaceObject
     */
    @Override
    public void registeInterface(String name, Object interfaceObject) {
        if (interfaceObject == null) {
            return;
        }
        className2InnerInterface.put(name, interfaceObject);
        className2InnerInterface.put(name.toUpperCase(), interfaceObject);
    }

    /**
     * 注册函数 FIXME UDAF的注册及发现逻辑移动到这里
     *
     * @param bean
     */
    @Override
    public void registeFunction(Object bean) {
        List<Method> methods = getMethodList(bean);
        Annotation udaf = bean.getClass().getAnnotation(UDAFFunction.class);
        FunctionType functionType = FunctionType.UDF;
        if (udaf != null) {
            functionType = FunctionType.UDAF;
            String name = ((UDAFFunction) udaf).value();
            this.className2InnerInterface.put(name, bean);
            this.className2InnerInterface.put(name.toUpperCase(), bean);
            return;
        }

        for (Method method : methods) {
            FunctionMethod annotation = method.getAnnotation(FunctionMethod.class);
            String functionName = annotation.value();
            // if (StringUtil.isEmpty(functionName)) {
            // functionName = method.getName();
            //
            registeFunction(functionName, bean, method, functionType);
            String comment = annotation.comment();
            // 获取类所在的包名
            String packageName = bean.getClass().getPackage().getName();
            String group = packageName.substring(packageName.lastIndexOf(".") + 1);
            // 获取参数描述信息
            String params = getParamsComment(method);
            // 注册函数的参数信息
            //registeFunctionParam(functionName, params, comment, method.getGenericReturnType().toString(), group);
            if (StringUtil.isNotEmpty(annotation.alias())) {
                String aliases = annotation.alias();
                if (aliases.indexOf(",") != -1) {
                    String[] values = aliases.split(",");
                    for (String alias : values) {
                        registeFunction(alias, bean, method, functionType);
                        //registeFunctionParam(alias, params, comment, method.getGenericReturnType().toString(),
                        //    group);
                    }
                } else {
                    registeFunction(aliases, bean, method, functionType);
                    //registeFunctionParam(aliases, params, comment, method.getGenericReturnType().toString(),
                    //    group);
                }

            }

        }

    }

    /**
     * 获取参数列表中的注册信息
     *
     * @param method
     * @return
     */
    private String getParamsComment(Method method) {
        StringBuilder result = new StringBuilder();
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        if (parameterAnnotations != null && parameterAnnotations.length > 0) {
            for (Annotation[] parameterAnnotation : parameterAnnotations) {
                for (Annotation annotation : parameterAnnotation) {
                    if (annotation instanceof FunctionParamter) {
                        FunctionParamter param = (FunctionParamter) annotation;
                        result.append(param.comment() + "@" + param.value() + ",");
                    }
                }
            }
        }
        if (result.length() > 0) {
            result.deleteCharAt(result.length() - 1);
        }
        return result.toString();
    }

    @Override
    public boolean isUDF(String functionName) {
        FunctionConfigure functionConfigure = getFunctionConfigure(functionName);
        return functionConfigure.isUDF();
    }

    @Override
    public boolean isUDAF(String functionName) {
        FunctionConfigure functionConfigure = getFunctionConfigure(functionName);
        return functionConfigure.isUDAF();
    }

    @Override
    public boolean isUDTF(String functionName) {
        FunctionConfigure functionConfigure = getFunctionConfigure(functionName);
        return functionConfigure.isUDTF();
    }

    /**
     * 执行一个函数，默认会增加上下文和message
     *
     * @param functionName
     * @param parameters
     * @param <T>
     * @return
     */
    @Override
    public <T> T executeFunction(IMessage message, FunctionContext context, String functionName, Object... parameters) {
        IDipperInterfaceAdpater adpater = functionName2Adapter.get(functionName);
        if (adpater != null) {
            return doDipperInterface(message, context, adpater, parameters);
        }
        Object[] allParameters = createAllParameter(message, context, parameters);
        FunctionConfigure functionConfigure = getFunctionConfigure(functionName, allParameters);
        if (functionConfigure == null) {
            LOGGER.warn("not found engine for " + functionName);
            return null;
        }
        return directExecuteFunction(functionConfigure, allParameters);
    }

    @Override
    public FunctionConfigure getFunctionConfigure(IMessage message, FunctionContext context, String functionName,
        Object... parameters) {
        Object[] allParameters = createAllParameter(message, context, parameters);
        return getFunctionConfigure(functionName, allParameters);
    }

    /**
     * 在parameters基础上增加message和context
     *
     * @param message
     * @param context
     * @param parameters
     * @return
     */
    private Object[] createAllParameter(IMessage message, FunctionContext context, Object... parameters) {
        Object[] allParameters = new Object[parameters.length + 2];
        allParameters[0] = message;
        allParameters[1] = context;
        for (int i = 2; i < allParameters.length; i++) {
            allParameters[i] = parameters[i - 2];
        }
        return allParameters;
    }

    @Override
    public FunctionConfigure getFunctionConfigure(String functionName, Object... allParameters) {
        FunctionConfigureMap functionConfigureMap = functionName2Engies.get(functionName);
        if (functionConfigureMap == null) {
            //LOG.warn("get function may be not registe engine for " + functionName);
            return null;
        }
        FunctionConfigure engine = functionConfigureMap.getFunction(allParameters);
        if (engine == null) {
            //LOG.warn("get engine may be not registe engine for " + functionName);
            return null;
        }
        return engine;
    }

    @Override public DataType getReturnDataType(String functionName) {
        FunctionConfigureMap functionConfigureMap = functionName2Engies.get(functionName);
        if (functionConfigureMap == null) {
            LOGGER.warn("get function may be not registe engine for " + functionName);
            return null;
        }
        List<FunctionConfigure> functionConfigureList = functionConfigureMap.getFunctionConfigureList();
        if (functionConfigureList == null || functionConfigureList.size() == 0) {
            return null;
        }
        DataType returnDataType = null;
        for (FunctionConfigure functionConfigure : functionConfigureList) {
            if (returnDataType == null) {
                returnDataType = functionConfigure.getReturnDataType();
                continue;
            }
            if (!returnDataType.getDataTypeName().equals(functionConfigure.getReturnDataType().getDataTypeName())) {
                throw new RuntimeException("can not return returnDataType , the FunctionConfigureMap has different returnDataType, the funtcion is " + functionName);
            }
        }
        return null;
    }

    @Override
    public <T> T directExecuteFunction(String functionName, Object... allParameters) {
        FunctionConfigure engine = getFunctionConfigure(functionName, allParameters);
        if (engine == null) {
            throw new RuntimeException(
                "' doExecute " + functionName + "' function not regist, maybe function name is error ");
        }
        return directExecuteFunction(engine, allParameters);
    }

    protected <T> T directExecuteFunction(FunctionConfigure engine, Object... allParameters) {
        return (T) engine.execute(allParameters);
    }

    @Override
    public boolean startWith(String functionName, Class[] classes) {
        FunctionConfigureMap functionConfigureMap = functionName2Engies.get(functionName);
        if (functionConfigureMap == null) {
            LOGGER.warn("startWith may be not registe engine for " + functionName);
            return false;
        }
        return functionConfigureMap.startWith(classes);
    }

    private <T> T doDipperInterface(IMessage message, FunctionContext context, IDipperInterfaceAdpater adpater,
        Object[] parameters) {
        JSONObject jsonObject = new JSONObject();

        for (int i = 0; i < adpater.count(); i++) {
            if (i >= parameters.length) {
                break;
            }
            String name = adpater.getParamterName(i);
            if (parameters[i] == null) {
                continue;
            }
            jsonObject.put(name, FunctionUtils.getValue(message, context, parameters[i].toString()));
        }
        T t = (T) adpater.doScript(jsonObject.toJSONString());
        dealBreakExecute(context, t);
        return t;
    }

    private <T> void dealBreakExecute(FunctionContext context, T t) {
        try {
            if (t instanceof Map) {
                if (StringUtil.equals((((Map) t).get("breakExecute") + ""), "1")) {
                    context.breakExecute();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取所有带标注的方法
     *
     * @param o
     * @return
     */
    private List<Method> getMethodList(Object o) {
        Method[] methods = o.getClass().getMethods();
        List<Method> methodList = new ArrayList<Method>();
        for (Method method : methods) {
            if (method.getAnnotation(FunctionMethod.class) != null) {
                methodList.add(method);
            }
        }
        return methodList;
    }

    @Override
    public <T> T getInnerInteface(String interfacName) {
        if (!className2InnerInterface.containsKey(interfacName)) {
            return null;
        }
        return (T) className2InnerInterface.get(interfacName);
    }

    public Map<String, FunctionConfigureMap> getFunctionName2Engies() {
        return functionName2Engies;
    }

    public Map<String, Object> getClassName2InnerInterface() {
        return className2InnerInterface;
    }

    public Map<String, FunctionInfoMap> getFunctionParam2Engies() {
        return functionParam2Engies;
    }

    protected class IngoreInsensitiveMap<V> extends HashMap<String, V> {

        @Override
        public V get(Object key) {
            if (key == null) {
                return null;
            }
            String keyString = (String) key;
            keyString = keyString.toLowerCase();
            return super.get(keyString);
        }

        @Override
        public V put(String key, V value) {
            if (key == null) {
                return value;
            }
            String keyString = key.toLowerCase();
            return super.put(keyString, value);
        }
    }

}
