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
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class RandFunction {

    /**
     * 返回随机数
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "rand", comment = "返回随机数")
    public Double rand(IMessage message, FunctionContext context) {
        Double result = null;
        result = Math.random();
        return result;
    }

    /**
     * 返回随机数
     *
     * @param message
     * @param context
     * @param number
     * @return
     */
    @FunctionMethod(value = "rand", comment = "返回随机数")
    public Double rand(IMessage message, FunctionContext context, String number) {
        Double result = null;
        Double tem = FunctionUtils.getValueDouble(message, context, number);
        if (number == null) {
            return result;
        }
        result = Math.random() + tem;
        return result;
    }
}
