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
package org.apache.rocketmq.streams.script.function.service;

import java.lang.reflect.Method;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.model.FunctionType;

/**
 * 函数管理，java的一个方法可以发布成一个函数 可以通过函数名和参数，完成函数的调用
 */
public interface IFunctionService {

    /**
     * 注册一个函数
     *
     * @param functionName 函数名称
     * @param method       bean中的方法
     */
    void registeFunction(String functionName, Object bean, Method method);

    /**
     * 注册一个函数
     *
     * @param functionName 函数名称
     * @param method       bean中的方法
     * @param functionType 目前主要是展示，主要在兼容blink/spark udf时使用
     */
    void registeFunction(String functionName, Object bean, Method method, FunctionType functionType);

    /**
     * 给一个接口取名字，并注册。 可以通过名字获取接口，单不支持通过函数名调用
     *
     * @param functionName    函数名称
     * @param interfaceObject 接口实现类
     */
    void registeInterface(String functionName, Object interfaceObject);

    /**
     * 感觉函数名寻找注册的接口。udaf是通过接口返回实例来实现
     *
     * @param funcitonName
     * @return
     */
    <T> T getInnerInteface(String funcitonName);

    /**
     * 只要bean中的方法做了@FunctionMethod标注，都会自动被注册
     *
     * @param bean 一个java对象
     */
    void registeFunction(Object bean);

    /**
     * 获取FunctionConfigure对象，这个对象能完成函数的反射执行 因为函数支持重载，同名函数有多个，需要把函数参数传过来，既比较名称，也比较参数及类型 很多函数实现默认带message，context。前两个参数只会寻找带这两个前缀参数的函数
     *
     * @param message
     * @param context
     * @param functionName 函数名称
     * @param parameters   函数参数
     * @return
     */
    FunctionConfigure getFunctionConfigure(IMessage message, FunctionContext context, String functionName,
        Object... parameters);

    /**
     * 获取FunctionConfigure对象，这个对象能完成函数的反射执行 因为函数支持重载，同名函数有多个，需要把函数参数传过来，既比较名称，也比较参数及类型 寻找不带message，context前缀参数的函数
     *
     * @param functionName 函数名称
     * @param parameters   函数参数
     * @return
     */
    FunctionConfigure getFunctionConfigure(String functionName, Object... parameters);

    /**
     * 执行函数，或先找到合适的FunctionConfigure，然后执行反射。函数带message，context前缀
     *
     * @param message
     * @param context
     * @param functionName 函数名称
     * @param parameters   函数参数
     * @param <T>
     * @return 函数执行返回值
     */
    <T> T executeFunction(IMessage message, FunctionContext context, String functionName, Object... parameters);

    /**
     * 执行函数，或先找到合适的FunctionConfigure，然后执行反射。函数不带message，context前缀
     *
     * @param functionName 函数名称
     * @param parameters   函数参数
     * @param <T>
     * @return 函数执行返回值
     */
    <T> T directExecuteFunction(String functionName, Object... parameters);

    /**
     * 是否有前缀是有前几个参数是classes 类型的函数
     *
     * @param classes, 参数列表的前几个参数的类型
     * @return
     */
    boolean startWith(String function, Class... classes);

    boolean isUDF(String functionName);

    boolean isUDTF(String functionName);

    boolean isUDAF(String functionName);

}
