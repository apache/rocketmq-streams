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
package org.apache.rocketmq.streams.script.service.udf;

import java.lang.reflect.Method;
import org.apache.rocketmq.streams.common.model.BeanHolder;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.service.IFunctionService;
import org.apache.rocketmq.streams.script.service.IAccumulator;

/**
 * 实现了IAccumulator。核心思路是通过反射，把外部的udaf 映射成标准的IAccumulator接口
 */
public abstract class UDAFScript<T, ACC> extends UDFScript implements IAccumulator<T, ACC> {

    protected String createAccumulatorMethodName;//创建累计器的方法名

    protected String getValueMethodName;//获取累计值的方法名

    protected String accumulateMethodName;//计算的方法名

    protected String mergeMethodName;//merge的方法名

    protected String retractMethodName;// retract的方法名

    protected transient FunctionConfigure createAccumulatorFunctionConfigure;

    protected transient FunctionConfigure getValueFunctionConfigure;

    protected transient FunctionConfigure accumulateFunctionConfigure;

    protected transient FunctionConfigure mergeFunctionConfigure;

    protected transient FunctionConfigure retractFunctionConfigure;

    /**
     * 是否完成初始化
     */
    private transient volatile boolean hasInit = false;

    @Override
    public void registFunctionSerivce(IFunctionService iFunctionService) {
        if (StringUtil.isEmpty(functionName) || hasInit) {
            return;
        }

        if (initBeanClass(iFunctionService)) {
            BeanHolder<String> error = new BeanHolder();
            iFunctionService.registeInterface(this.functionName, this);
            hasInit = true;
        }
    }


    @Override
    protected boolean initBeanClass(IFunctionService iFunctionService) {
        super.initBeanClass(iFunctionService);
        initMethod();
        return true;
    }

    public void initMethod() {
        Method[] methods = this.instance.getClass().getMethods();
        for (Method method : methods) {
            if (method.getName().equals(createAccumulatorMethodName)) {
                this.createAccumulatorFunctionConfigure = new FunctionConfigure(this.functionName, method, instance);
            } else if (method.getName().equals(getValueMethodName)) {
                this.getValueFunctionConfigure = new FunctionConfigure(this.functionName, method, instance);
            } else if (method.getName().equals(accumulateMethodName)) {
                this.accumulateFunctionConfigure = new FunctionConfigure(this.functionName, method, instance);
            } else if (method.getName().equals(retractMethodName)) {
                this.retractFunctionConfigure = new FunctionConfigure(this.functionName, method, instance);
            } else if (method.getName().equals(mergeMethodName)) {
                this.mergeFunctionConfigure = new FunctionConfigure(this.functionName, method, instance);
            }
        }
    }

    @Override
    public ACC createAccumulator() {
        FunctionConfigure functionConfigure = getCreateAccumulatorFunctionConfigure();
        if (functionConfigure != null) {
            return (ACC)functionConfigure.execute(new Object[0]);
        }
        return null;
    }

    @Override
    public T getValue(ACC accumulator) {
        FunctionConfigure functionConfigure = getGetValueFunctionConfigure();
        if (functionConfigure != null) {
            return (T)functionConfigure.execute(new Object[] {accumulator});
        }
        return null;
    }

    @Override
    public void accumulate(ACC accumulator, Object... paramters) {
        FunctionConfigure functionConfigure = getAccumulateFunctionConfigure();
        if (functionConfigure != null) {
            Object[] realParamters = new Object[paramters.length + 1];
            realParamters[0] = accumulator;
            for (int i = 0; i < paramters.length; i++) {
                realParamters[i + 1] = paramters[i];
            }
            functionConfigure.execute(realParamters);
        }
    }

    public void accumulate(ACC accumulator, String... paramters) {
        Object[] datas = convert(paramters);
        accumulate(accumulator, datas);
    }

    protected Object[] convert(String... parameters) {
        if (parameters == null) {
            return new Object[0];
        }
        Object[] datas = new Object[parameters.length];
        for (int i = 0; i < parameters.length; i++) {
            //第一个参数是内置的
            datas[i] = accumulateFunctionConfigure.getParameterDataTypes()[i + 1].getData(parameters[i]);
        }
        return datas;
    }

    protected abstract Object createMergeParamters(Iterable<ACC> its);

    @Override
    public void merge(ACC accumulator, Iterable<ACC> its) {
        FunctionConfigure functionConfigure = getMergeFunctionConfigure();
        if (functionConfigure != null) {
            functionConfigure.execute(new Object[] {accumulator, createMergeParamters(its)});
        }
    }

    @Override
    public void retract(ACC accumulator, String... paramters) {
        FunctionConfigure functionConfigure = getRetractFunctionConfigure();
        if (functionConfigure != null) {
            Object[] datas = convert(paramters);
            Object[] realParamters = new Object[paramters.length + 1];
            realParamters[0] = accumulator;
            for (int i = 0; i < datas.length; i++) {
                realParamters[i + 1] = datas[i];
            }
            functionConfigure.execute(realParamters);
        }
    }

    public FunctionConfigure getCreateAccumulatorFunctionConfigure() {
        return createAccumulatorFunctionConfigure;
    }

    public FunctionConfigure getGetValueFunctionConfigure() {
        return getValueFunctionConfigure;
    }

    public FunctionConfigure getAccumulateFunctionConfigure() {
        return accumulateFunctionConfigure;
    }

    public FunctionConfigure getMergeFunctionConfigure() {
        return mergeFunctionConfigure;
    }

    public FunctionConfigure getRetractFunctionConfigure() {
        return retractFunctionConfigure;
    }

    public String getCreateAccumulatorMethodName() {
        return createAccumulatorMethodName;
    }

    public void setCreateAccumulatorMethodName(String createAccumulatorMethodName) {
        this.createAccumulatorMethodName = createAccumulatorMethodName;
    }

    public String getGetValueMethodName() {
        return getValueMethodName;
    }

    public void setGetValueMethodName(String getValueMethodName) {
        this.getValueMethodName = getValueMethodName;
    }

    public String getAccumulateMethodName() {
        return accumulateMethodName;
    }

    public void setAccumulateMethodName(String accumulateMethodName) {
        this.accumulateMethodName = accumulateMethodName;
    }

    public String getMergeMethodName() {
        return mergeMethodName;
    }

    public void setMergeMethodName(String mergeMethodName) {
        this.mergeMethodName = mergeMethodName;
    }

    public String getRetractMethodName() {
        return retractMethodName;
    }

    public void setRetractMethodName(String retractMethodName) {
        this.retractMethodName = retractMethodName;
    }
}
