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
public class SinFunction {

    /**
     * 正弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "sin", comment = "求正弦函数值")
    public Double sin(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "String", comment = "代表要求的值的列名称或常量值") String number) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, number));
        if (numberTem == null) {
            return result;
        }
        result = Math.sin(numberTem);
        return result;
    }

    /**
     * 正弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "sin", comment = "求正弦函数值")
    public Double sin(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "Double", comment = "代表要求的值的常量值") Double number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.sin(number);
        return result;
    }

    /**
     * 正弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "sin", comment = "求正弦函数值")
    public Double sin(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "Integer", comment = "代表要求的值的常量值") Integer number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.sin(number);
        return result;
    }

    /**
     * 正弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "sin", comment = "求正弦函数值")
    public BigDecimal sin(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "BigDecimal", comment = "代表要求的值的常量值") BigDecimal number) {
        BigDecimal result = null;
        if (number == null) {
            return result;
        }
        result = new BigDecimal(Math.sin(number.intValue()));
        return result;
    }
}
