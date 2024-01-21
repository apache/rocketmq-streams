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
package org.apache.rocketmq.streams.script.function.impl.condition;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class InConditionFunction {

    @FunctionMethod(value = "contain", comment = "判断某个字符串是否在一个列表中")
    public boolean nin(IMessage message, FunctionContext context, String fieldName, String... conditions) {
        return nin(message, context, fieldName, false, conditions);
    }

    @FunctionMethod(value = "!contain", comment = "判断某个字符串是否在一个列表中")
    public boolean notin(IMessage message, FunctionContext context, String fieldName, String... conditions) {
        return !nin(message, context, fieldName, false, conditions);
    }

    @FunctionMethod(value = "in", alias = "contain", comment = "判断某个字符串是否在一个列表中")
    public boolean nin(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表待比较字符串的字段名或常量") String fieldName,
        @FunctionParamter(value = "boolean", comment = "是否支持正则匹配") Boolean isRegexMatch,
        @FunctionParamter(value = "array", comment = "代表字符串的字段名或常量列表") String... conditions) {

        fieldName = FunctionUtils.getConstant(fieldName);
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || conditions == null) {
            return false;
        }
        for (String condition : conditions) {
            String conditonValue = FunctionUtils.getValueString(message, context, condition);
            if (StringUtil.isEmpty(conditonValue)) {
                continue;
            }
            if (ori.equals(conditonValue)) {
                return true;
            }
            if (isRegexMatch && StringUtil.matchRegex(ori, conditonValue)) {
                return true;
            }
        }
        return false;
    }

    @FunctionMethod(value = "in", comment = "判断某个字符串是否在一个列表中")
    public boolean match(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名称，不加引号") String fieldName,
        @FunctionParamter(value = "array", comment = "常量列表，不加引号") String... conditions) {
        String ori = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        if (StringUtil.isEmpty(ori) || conditions == null) {
            return false;
        }
        for (String condition : conditions) {
            if (condition.equals(ori)) {
                return true;
            }
        }
        return false;
    }

    @FunctionMethod(value = "inByRegex", comment = "判断某个字符串是否在一个列表中")
    public boolean matchByRegex(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名称，不加引号") String fieldName,
        @FunctionParamter(value = "array", comment = "正则列表，不加引号") String... conditions) {
        String ori = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        if (StringUtil.isEmpty(ori) || conditions == null) {
            return false;
        }
        for (String condition : conditions) {
            if (!StringUtil.matchRegex(ori, condition)) {
                return true;
            }
        }
        return false;
    }

}
