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

import org.apache.rocketmq.streams.common.context.IMessage;

@Function
public class BinFunction {

    /**
     * 返回number的二进制代码表示
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "bin", alias = "bin", comment = "返回number的二进制代码表示")
    public String bin(IMessage message, FunctionContext context,
                      @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String number) {
        String result = null;
        Integer numberTem = Integer.parseInt(FunctionUtils.getValueString(message, context, number));
        if (numberTem == null) {
            return result;
        }
        if (numberTem == 0) {
            return "0";
        }
        result = Integer.toBinaryString(numberTem);
        return result;
    }

}