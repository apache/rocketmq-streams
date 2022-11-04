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
public class TanFunction {

    /**
     * 正切函数，输入为弧度值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "tan", comment = "正切函数，输入为弧度值")
    public Double tan(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "String", comment = "代表要求值的列名称或常量值") String number) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, number));
        if (numberTem == null) {
            return result;
        }
        result = Math.tan(numberTem);
        return result;
    }

    /**
     * 正切函数，输入为弧度值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "tan", comment = "正切函数，输入为弧度值")
    public Double tan(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "double", comment = "代表要求值的常量值") Double number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.tan(number);
        return result;
    }

    /**
     * 正切函数，输入为弧度值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "tan", comment = "正切函数，输入为弧度值")
    public Double tan(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "integer", comment = "代表要求值的常量值") Integer number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.tan(number);
        return result;
    }

}
