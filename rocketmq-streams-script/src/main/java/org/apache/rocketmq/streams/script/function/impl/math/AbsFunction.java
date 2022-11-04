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

import java.math.BigDecimal;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class AbsFunction {

    /**
     * 求数值的绝对值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "abs", alias = "abs", comment = "求数值的绝对值")
    public Double abs(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "string", comment =
                          "Double或bigint类型或Decimal类型，输入为bigint时返回bigint，输入为double时返回double类型。输入decimal类型时返回decimal类型")
                          String number) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, number));
        if (numberTem == null) {
            return result;
        }
        result = Math.abs(numberTem);
        return result;
    }

    /**
     * 求数值的绝对值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "abs", alias = "abs", comment = "求数值的绝对值")
    public Double abs(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "Double", comment =
                          "Double或bigint类型或Decimal类型，输入为bigint时返回bigint，输入为double时返回double类型。输入decimal类型时返回decimal类型")
                          Double number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.abs(number);
        return result;
    }

    /**
     * 求数值的绝对值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "abs", alias = "abs", comment = "求数值的绝对值")
    public Integer abs(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Integer", comment =
                           "Double或bigint类型或Decimal类型，输入为bigint时返回bigint，输入为double时返回double类型。输入decimal类型时返回decimal类型")
                           Integer number) {
        Integer result = null;
        if (number == null) {
            return result;
        }
        result = Math.abs(number);
        return result;
    }



}
