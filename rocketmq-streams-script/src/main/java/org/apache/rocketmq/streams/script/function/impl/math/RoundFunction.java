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

import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.context.FunctionContext;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;

import java.math.BigDecimal;

import org.apache.rocketmq.streams.common.context.IMessage;

@Function
public class RoundFunction {

    /**
     * 四舍五入到指定小数点位置
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "round", comment = "四舍五入到指定小数点位置")
    public Double round(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表要求值的列名或常量") String base,
                        @FunctionParamter(value = "string", comment = "四舍五入计算到小数点后的位置") String x) {
        Double result = null;
        Double baseTem = Double.parseDouble(FunctionUtils.getValueString(message, context, base));
        Integer xTem = Integer.parseInt(FunctionUtils.getValueString(message, context, x));
        if (baseTem == null || xTem == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(baseTem);
        result = bg.setScale(xTem, BigDecimal.ROUND_HALF_UP).doubleValue();
        return result;
    }

    /**
     * 四舍五入到指定小数点位置
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "round", comment = "四舍五入到指定小数点位置")
    public Double round(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "Double", comment = "代表要求值的常量") Double base,
                        @FunctionParamter(value = "Double", comment = "四舍五入计算到小数点后的位置") Double x) {
        Double result = null;
        if (base == null || x == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(base);
        result = bg.setScale(x.intValue(), BigDecimal.ROUND_HALF_UP).doubleValue();
        return result;
    }

    /**
     * 四舍五入到指定小数点位置
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "round", comment = "四舍五入到指定小数点位置")
    public Double round(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "integer", comment = "代表要求值的常量") Integer base,
                        @FunctionParamter(value = "integer", comment = "四舍五入计算到小数点后的位置") Integer x) {
        Double result = null;
        if (base == null || x == null) {
            return result;
        }
        BigDecimal bg = new BigDecimal(base);
        result = bg.setScale(x.intValue(), BigDecimal.ROUND_HALF_UP).doubleValue();
        return result;
    }

    /**
     * 四舍五入到指定小数点位置
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "round", comment = "四舍五入到指定小数点位置")
    public BigDecimal round(IMessage message, FunctionContext context,
                            @FunctionParamter(value = "BigDecimal", comment = "代表要求值的常量") BigDecimal base,
                            @FunctionParamter(value = "BigDecimal", comment = "四舍五入计算到小数点后的位置") BigDecimal x) {
        BigDecimal result = null;
        if (base == null || x == null) {
            return result;
        }
        result = new BigDecimal(base.setScale(x.intValue(), BigDecimal.ROUND_HALF_UP).doubleValue());
        return result;
    }
}
