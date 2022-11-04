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
public class FloorFunction {

    /**
     * 向下取整，返回比number小的整数值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "floor", alias = "floor", comment = "向下取整，返回比当前值小的整数值")
    public Double floor(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量值") String number) {
        Double result = null;
        String value = FunctionUtils.getValueString(message, context, number);
        if (value == null) {
            return result;
        }
        try {
            Double numberTem = Double.parseDouble(value);
            if (numberTem == null) {
                return result;
            }
            result = Math.floor(numberTem);
        } catch (Exception e) {
            return null;
        }
        return result;
    }

    /**
     * 向下取整，返回比number小的整数值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "floor", alias = "floor", comment = "向下取整，返回比当前值小的整数值")
    public Double floor(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "Double", comment = "代表要求值的常量值") Double number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.floor(number);
        return result;
    }

    /**
     * 向下取整，返回比number小的整数值
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "floor", alias = "floor", comment = "向下取整，返回比当前值小的整数值")
    public Double floor(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "Integer", comment = "代表要求值的常量值") Integer number) {
        Double result = null;
        if (number == null) {
            return result;
        }
        result = Math.floor(number);
        return result;
    }


}
