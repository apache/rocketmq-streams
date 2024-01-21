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
public class LogFunction {

    /**
     * 返回以base为底的x的对数
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "log", alias = "log", comment = "返回以base为底的值的对数")
    public Double log(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "底数值") String base,
        @FunctionParamter(value = "String", comment = "要求对数的字段名或常量值") String x) {
        Double result = null;
        Double baseTem = Double.parseDouble(FunctionUtils.getValueString(message, context, base));
        Double xTem = Double.parseDouble(FunctionUtils.getValueString(message, context, x));
        if (baseTem == null || xTem == null) {
            return result;
        }
        result = Math.log(baseTem) / Math.log(xTem);
        return result;
    }

    /**
     * 返回以base为底的x的对数
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "log", alias = "log", comment = "返回以base为底的值的对数")
    public Double log(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Double", comment = "底数值") Double base,
        @FunctionParamter(value = "Double", comment = "要求对数值的常量") Double x) {
        Double result = null;
        if (base == null || x == null) {
            return result;
        }
        result = Math.log(base) / Math.log(x);
        return result;
    }

    /**
     * 返回以base为底的x的对数
     *
     * @param message
     * @param context
     * @param x
     * @return
     */
    @FunctionMethod(value = "log", alias = "log", comment = "返回以base为底的值的对数")
    public Double log(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "底数值") Integer base,
        @FunctionParamter(value = "Integer", comment = "要求对数值的常量") Integer x) {
        Double result = null;
        if (base == null || x == null) {
            return result;
        }
        result = Math.log(base) / Math.log(x);
        return result;
    }

}
