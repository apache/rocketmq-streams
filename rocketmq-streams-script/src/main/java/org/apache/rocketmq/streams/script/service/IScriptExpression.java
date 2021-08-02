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
package org.apache.rocketmq.streams.script.service;

import java.util.List;

import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.common.context.IMessage;

/**
 * 一个函数，如a=now();就是一个表达式 这里是函数真正执行的地方
 */
public interface IScriptExpression<T> extends IScriptParamter {

    /**
     * 执行一个函数
     *
     * @param message 消息
     * @param context 上下文
     * @return 函数返回值
     */
    @SuppressWarnings("rawtypes")
    T executeExpression(IMessage message, FunctionContext context);

    /**
     * 获取函数的参数，参数用IScriptParamter表示
     *
     * @return
     */
    List<IScriptParamter> getScriptParamters();

    /**
     * 获取函数名称
     *
     * @return
     */
    String getFunctionName();

    /**
     * 获取表达式的字符串。是未解析的函数形式。如"a=now();"
     *
     * @return
     */
    String getExpressionDescription();
}
