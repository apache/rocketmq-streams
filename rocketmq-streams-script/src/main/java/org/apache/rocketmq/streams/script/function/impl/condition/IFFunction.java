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
 */package org.apache.rocketmq.streams.script.function.impl.condition;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class IFFunction {

    @FunctionMethod(value = "if", alias = "case", comment = "支持内嵌函数")
    public Boolean match(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Boolean", comment = "常量") Boolean value) {
        return value;
    }

    //@FunctionMethod(value = "if", alias = "case", comment = "支持内嵌函数")
    //public Boolean match(IMessage message, FunctionContext context,
    //    @FunctionParamter(value = "String", comment = "代表字符串的字段名或常量") String value) {
    //    value = FunctionUtils.getValueString(message, context, value);
    //    return Boolean.valueOf(value);
    //}
}
