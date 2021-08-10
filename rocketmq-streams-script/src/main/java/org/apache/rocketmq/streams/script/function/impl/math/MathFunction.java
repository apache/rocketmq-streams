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
package org.apache.rocketmq.streams.script.function.impl.math;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class MathFunction {
    /**
     * 将输入值number截取到指定小数点位置
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "math", comment = "将输入值number截取到指定小数点位置")
    public Object mathOperator(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "String", comment = "代表要求值的列名称或常量值") String operator,
                               @FunctionParamter(value = "String", comment = "代表要截取小数点的位置") String a, String b) {
        Object aValue = FunctionUtils.getValue(message, context, a);
        Object bValue = FunctionUtils.getValue(message, context, b);
        if (Double.class.isInstance(aValue)) {
            return mathOperator(message, context, operator, (Double)aValue, (Double)bValue);
        } else if (Integer.class.isInstance(aValue)) {
            return mathOperator(message, context, operator, (Integer)aValue, (Integer)bValue);
        } else {
            return mathOperator(message, context, operator, (Long)aValue, (Long)bValue);
        }

    }

    /**
     * 将输入值number截取到指定小数点位置
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "mathl", comment = "将输入值number截取到指定小数点位置")
    public Long mathOperator(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "String", comment = "代表要求值的列名称或常量值") String operator,
                             @FunctionParamter(value = "String", comment = "代表要截取小数点的位置") Long a, Long b) {
        operator = FunctionUtils.getValueString(message, context, operator);
        if ("+".equals(operator)) {
            return a + b;
        } else if ("-".equals(operator)) {
            return a - b;
        } else if ("*".equals(operator)) {
            return a * b;
        } else if ("/".equals(operator)) {
            return a / b;
        } else if ("%".equals(operator)) {
            return a % b;
        } else {
            throw new RuntimeException("can not support this driver " + operator);
        }
    }

    /**
     * 将输入值number截取到指定小数点位置
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "mathi", comment = "将输入值number截取到指定小数点位置")
    public Integer mathOperator(IMessage message, FunctionContext context,
                                @FunctionParamter(value = "String", comment = "代表要求值的列名称或常量值") String operator,
                                @FunctionParamter(value = "String", comment = "代表要截取小数点的位置") Integer a, Integer b) {
        operator = FunctionUtils.getValueString(message, context, operator);
        if ("+".equals(operator)) {
            return a + b;
        } else if ("-".equals(operator)) {
            return a - b;
        } else if ("*".equals(operator)) {
            return a * b;
        } else if ("/".equals(operator)) {
            return a / b;
        } else if ("%".equals(operator)) {
            return a % b;
        } else {
            throw new RuntimeException("can not support this driver " + operator);
        }
    }

    /**
     * 将输入值number截取到指定小数点位置
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "mathd", comment = "将输入值number截取到指定小数点位置")
    public Double mathOperator(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "String", comment = "代表要求值的列名称或常量值") String operator,
                               @FunctionParamter(value = "String", comment = "代表要截取小数点的位置") Double a, Double b) {
        operator = FunctionUtils.getValueString(message, context, operator);
        if ("+".equals(operator)) {
            return a + b;
        } else if ("-".equals(operator)) {
            return a - b;
        } else if ("*".equals(operator)) {
            return a * b;
        } else if ("/".equals(operator)) {
            return a / b;
        } else if ("%".equals(operator)) {
            return a % b;
        } else {
            throw new RuntimeException("can not support this driver " + operator);
        }
    }

}
