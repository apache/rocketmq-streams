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
public class FactorialFunction {

    /**
     * 返回number的阶乘
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "factorial", alias = "factorial", comment = "返回值的阶乘")
    public Long factorial(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "Integer", comment = "代表要求值的常量值") Integer number) {
        Long result = null;
        if (number == null || number < 0 || number > 20) {
            return result;
        }
        if (number == 0) {
            result = 1L;
        }
        long m = 1;
        for (int j = 1; j <= number; j++) {
            m = m * j;
        }
        return result;
    }

    /**
     * 返回number的阶乘
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "factorial", alias = "factorial", comment = "返回值的阶乘")
    public Long factorial(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量值") String number) {
        Long result = null;
        Integer numberTem = Integer.parseInt(FunctionUtils.getValueString(message, context, number));
        if (number == null || numberTem < 0 || numberTem > 20) {
            return result;
        }
        if (numberTem == 0) {
            result = 1L;
        }
        long m = 1;
        for (int j = 1; j <= numberTem; j++) {
            m = m * j;
        }
        return result;
    }
}
