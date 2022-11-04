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
public class AcosFunction {

    /**
     * 求反余弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "acos", alias = "acos", comment = "求反余弦函数")
    public Double acos(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "String", comment = "代表要求反余弦的字段名或常量") String number) {
        Double result = null;
        Double numberTem = Double.parseDouble(FunctionUtils.getValueString(message, context, number));
        if (numberTem == null || numberTem < -1.0 || numberTem > 1.0) {
            return result;
        }
        result = Math.acos(numberTem);
        return result;
    }

    /**
     * 求反余弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "acos", alias = "acos", comment = "求反余弦")
    public Double acos(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Double", comment = "Double常量") Double number) {
        Double result = null;
        if (number == null || number < -1 || number > 1) {
            return result;
        }
        result = Math.acos(number);
        return result;
    }


    /**
     * 求反余弦函数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "acos", alias = "acos", comment = "求反余弦")
    public Double acos(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "Integer", comment = "Integer常量") Integer number) {
        Double result = null;
        if (number == null || number < -1 || number > 1) {
            return result;
        }
        result = Math.acos(number);
        return result;
    }
}
