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

/**
 * 乘法函数支持
 */
@Function
public class MultiplicationFunction {

    /**
     * 返回两个 数的乘法结果
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "multiplication", alias = "multiplication", comment = "两个数值的乘法返回结果")
    public Object multiplication(IMessage message, FunctionContext context,
                                 @FunctionParamter(value = "String", comment = "代表要求乘法的第一个参数") String x,
                                 @FunctionParamter(value = "String", comment = "代表要求乘法的第二个参数") String y) {
        Double result = null;
        String paramX = FunctionUtils.getValueString(message, context, x);
        String paramY = FunctionUtils.getValueString(message, context, y);
        if (paramX == null || paramY == null) {
            return result;
        }
        Double baseTem = Double.parseDouble(paramX);
        Double xTem = Double.parseDouble(paramY);

        BigDecimal b1 = new BigDecimal(String.valueOf(baseTem));
        BigDecimal b2 = new BigDecimal(String.valueOf(xTem));
        BigDecimal bb = b1.multiply(b2);
        BigDecimal res = bb.setScale(2, BigDecimal.ROUND_HALF_UP);

        return res.doubleValue();

    }

}
