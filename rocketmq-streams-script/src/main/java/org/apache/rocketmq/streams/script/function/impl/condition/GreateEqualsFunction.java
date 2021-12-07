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

import java.math.BigDecimal;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class GreateEqualsFunction {
    @FunctionMethod(value = "greateeq", alias = ">=", comment = ">=")
    public Boolean match(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName,
                         @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String value) {
        String leftValue = FunctionUtils.getValueString(message, context, fieldName);
        if (FunctionUtils.isConstant(value)) {
            //support varchar and int transfer automatically
            try {
                BigDecimal right = new BigDecimal(FunctionUtils.getConstant(value));
                BigDecimal left = new BigDecimal(leftValue);
                return left.compareTo(right) >= 0;
            } catch (Exception e) {
                return leftValue.compareTo(FunctionUtils.getConstant(value)) >= 0;
            }
        } else if (FunctionUtils.isLong(value)) {
            Long left = FunctionUtils.getLong(leftValue.toString());
            Long right = FunctionUtils.getLong(value);
            return left >= right;
        } else if (FunctionUtils.isDouble(value)) {
            Double left = FunctionUtils.getDouble(leftValue.toString());
            Double right = FunctionUtils.getDouble(value);
            return (left >= right);
        } else {
            String right = FunctionUtils.getValueString(message, context, value);
            String left = leftValue.toString();
            return left.compareTo(right) >= 0;
        }
    }

    @Deprecated
    @FunctionMethod(value = "ge", comment = "建议用greateeq或>=")
    public boolean ge(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName,
                      @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String value) {
        String fieldValue = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        return fieldValue.compareTo(value) >= 0;
    }

    @Deprecated
    @FunctionMethod(value = "geByField", comment = "建议用greateeq或>=")
    public boolean geByField(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName,
                             @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String otherFieldName) {
        String ori = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        String dest = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), otherFieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(dest)) {
            return false;
        }
        return ori.compareTo(dest) >= 0;
    }

}
