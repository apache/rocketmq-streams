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

import com.alibaba.fastjson.JSONArray;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class EqualsFunction {
    private final static String NULL = "null";

    public static boolean isEquals(String functionName) {
        if ("equal".equals(functionName) || "==".equals(functionName) || "equals".equals(functionName)) {
            return true;
        }
        return false;
    }

    public static boolean isNotEquals(String functionName) {
        if ("!equals".equals(functionName)) {
            return true;
        }
        return false;
    }

    @FunctionMethod(value = "equal", alias = "==", comment = "判断两个值是否相等")
    public Boolean match(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "字段名称或常量") String fieldName,
                         @FunctionParamter(value = "string", comment = "字段名称或常量") String value) {
        String leftValue = FunctionUtils.getValueString(message, context, fieldName);
        if (leftValue == null && value == null || (NULL.equals(value.toLowerCase()))) {
            return true;
        }
        //FIXME 感觉这样解不合理，解析equals函数的时候应该将value置成空
        if ("".equals(leftValue) && ("".equals(value) || "''".equals(value))) {
            return true;
        }
        if (StringUtil.isEmpty(leftValue)) {
            return false;
        }
        if (FunctionUtils.isConstant(value)) {
            boolean result = leftValue.equals(FunctionUtils.getConstant(value));
            return result;
        } else if (FunctionUtils.isLong(value)) {
            Long left = FunctionUtils.getLong(leftValue);
            Long right = FunctionUtils.getLong(value);
            return left == right;
        } else if (FunctionUtils.isDouble(value)) {
            Double left = FunctionUtils.getDouble(leftValue);
            Double right = FunctionUtils.getDouble(value);
            return Math.abs(left - right) < 0.001;
        } else {
            String right = FunctionUtils.getValueString(message, context, value);
            return leftValue.equals(right);
        }

    }

    @Deprecated
    @FunctionMethod(value = "equals", comment = "判断两个值是否相等")
    public boolean equals(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "字段名称") String fieldName,
                          @FunctionParamter(value = "string", comment = "常量，不加引号也默认为常量") String value) {

        return match(message, context, fieldName, value);
    }

    @Deprecated
    @FunctionMethod(value = "!equals", comment = "判断两个值是否相等")
    public boolean notEquals(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "字段名称") String fieldName,
                             @FunctionParamter(value = "string", comment = "常量，不加引号也默认为常量") String value) {
        return !equals(message, context, fieldName, value);
    }

    @Deprecated
    @FunctionMethod(value = "equalsByField", comment = "判断两个值是否相等")
    public boolean equalsByField(IMessage message, FunctionContext context,
                                 @FunctionParamter(value = "string", comment = "字段名称") String fieldName,
                                 @FunctionParamter(value = "string", comment = "字段名称") String otherFieldName) {
        String ori = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        String dest = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), otherFieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(dest)) {
            return false;
        }
        return ori.equals(dest);
    }

    /**
     * 判断一个字段是否为空
     *
     * @param channelMessage
     * @param context
     * @param fieldName
     */
    @FunctionMethod(value = "isFieldEmpty", comment = "判断一个字段是否为空")
    public boolean isFieldEmpty(IMessage channelMessage, FunctionContext context,
                                @FunctionParamter(value = "string", comment = "字段名称") String fieldName, String noUseValue) {
        Object value = FunctionUtils.getValue(channelMessage, context, fieldName);
        if (value == null) {
            return true;
        }
        if (String.class.isInstance(value) && StringUtil.isEmpty((String)value)) {
            return true;
        }
        if (JSONArray.class.isInstance(value) && ((JSONArray)value).size() == 0) {
            return true;
        }
        return false;

    }

    @FunctionMethod(value = "isNull", alias = "null", comment = "判断值是否为NULL")
    public boolean isNull(IMessage channelMessage, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "字段名称") String fieldName) {
        Object value = FunctionUtils.getValue(channelMessage, context, fieldName);
        if (value == null) {
            return true;
        }
        if (String.class.isInstance(value) && StringUtil.isEmpty((String)value)) {
            return true;
        }
        if (String.class.isInstance(value) && ((String)value).toLowerCase().equals("null")) {
            return true;
        }
        return false;
    }

    @FunctionMethod(value = "isNotNull", alias = "!null", comment = "判断值是否不为NULL")
    public boolean isNotNull(IMessage channelMessage, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "字段名称") String fieldName) {
        return !isNull(channelMessage, context, fieldName);
    }
}
