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

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;

/**
 * 函数的参数
 */
public interface IScriptParamter extends IFunctionFieldDependent {

    /**
     * 如果参数是嵌套函数，会执行完成，返回执行结果 如果参数是表达式，会完成结算，返回结果 如果参数是简单形式，直接返回参数
     *
     * @param message 消息
     * @param context 上下文
     * @return 参数结果
     */
    Object getScriptParamter(IMessage message, FunctionContext context);

    /**
     * 参数的字符串形式，未解析的原始字符串
     *
     * @return
     */
    String getScriptParameterStr();

}
