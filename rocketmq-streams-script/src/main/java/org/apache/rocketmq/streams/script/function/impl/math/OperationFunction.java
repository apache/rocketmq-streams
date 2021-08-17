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
public class OperationFunction {

    /**
     * 返回A + B的结果
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "add", alias = "add", comment = "返回A + B的结果")
    public Integer add(IMessage message, FunctionContext context,
                       @FunctionParamter(value = "String", comment = "第一个操作数") String a,
                       @FunctionParamter(value = "String", comment = "第二个操作数") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, b));
        if (a == null || b == null) {
            return result;
        }
        result = aTem + bTem;
        return result;
    }

    /**
     * 返回A – B的结果
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "minus ", alias = "minus", comment = "返回A – B的结果")
    public Integer minus(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "String", comment = "第一个操作数") String a,
                         @FunctionParamter(value = "String", comment = "第二个操作数") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, b));
        if (a == null || b == null) {
            return result;
        }
        result = aTem - bTem;
        return result;
    }

    /**
     * 返回A * B的结果。
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "multiply", alias = "multiply", comment = "返回A * B的结果")
    public Integer multiply(IMessage message, FunctionContext context,
                            @FunctionParamter(value = "String", comment = "第一个操作数") String a,
                            @FunctionParamter(value = "String", comment = "第二个操作数") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, b));
        if (a == null || b == null) {
            return result;
        }
        result = aTem - bTem;
        return result;
    }

    /**
     * 返回A / B的结果。
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "divide", alias = "divide", comment = "返回A / B的结果")
    public Integer divide(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "String", comment = "第一个操作数") String a,
                          @FunctionParamter(value = "String", comment = "第二个操作数") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, b));
        if (a == null || b == null || bTem == 0) {
            return result;
        }
        result = aTem - bTem;
        return result;
    }

}
