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
import java.math.RoundingMode;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class BroundFunction {

    /**
     * 返回银行家舍入法的值
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "bround", alias = "bround", comment = "返回银行家舍入法的值")
    public Double bround(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "代表要求值的integer常量") Integer base) {
        Double result = null;
        if (base == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(base);
        result = bg.setScale(1, RoundingMode.HALF_EVEN).doubleValue();
        return result;
    }

    /**
     * 返回银行家舍入法的值
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "bround", alias = "bround", comment = "返回银行家舍入法的值")
    public Double bround(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String base) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, base));
        if (base == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(numberTem);
        result = bg.setScale(1, RoundingMode.HALF_EVEN).doubleValue();
        return result;
    }

    /**
     * 返回银行家舍入法的值
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "bround", alias = "bround", comment = "返回银行家舍入法的值")
    public Double bround(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "代表要求值的integer常量") Integer base,
        @FunctionParamter(value = "Integer", comment = "银行家舍入法计算后保留的小数位数") Integer x) {
        Double result = null;
        if (base == null || x == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(base);
        result = bg.setScale(x.intValue(), RoundingMode.HALF_EVEN).doubleValue();
        return result;
    }

    /**
     * 返回银行家舍入法的值
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "bround", alias = "bround", comment = "返回银行家舍入法的值")
    public Double bround(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String base,
        @FunctionParamter(value = "Integer", comment = "银行家舍入法计算后保留的小数位数") Integer x) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, base));
        if (base == null || x == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(numberTem);
        result = bg.setScale(x.intValue(), RoundingMode.HALF_EVEN).doubleValue();
        return result;
    }
}
