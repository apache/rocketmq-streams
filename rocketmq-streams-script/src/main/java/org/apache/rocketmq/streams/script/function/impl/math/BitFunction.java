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
public class BitFunction {

    /**
     * 位取或，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitor", alias = "bitor", comment = "位取或，输入和输出类型均为整型，且类型一致")
    public Integer or(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "取位或的第一个值") Integer a,
        @FunctionParamter(value = "Integer", comment = "取位或的第二个值") Integer b) {
        Integer result = null;
        if (a == null || b == null) {
            return result;
        }
        result = a | b;
        return result;
    }

    /**
     * 位取或，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitor", alias = "bitor", comment = "位取或，输入和输出类型均为整型，且类型一致")
    public Integer or(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String a,
        @FunctionParamter(value = "String", comment = "代表要求反余弦的字段名或常量") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        if (a == null || b == null) {
            return result;
        }
        result = aTem | bTem;
        return result;
    }

    /**
     * 按位取异或，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitxor", alias = "bitxor", comment = "按位取异或")
    public Integer xor(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "取位或的第一个值") Integer a,
        @FunctionParamter(value = "Integer", comment = "取位或的第一个值") Integer b) {
        Integer result = null;
        if (a == null || b == null) {
            return result;
        }
        result = a ^ b;
        return result;
    }

    /**
     * 按位取异或，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitxor", alias = "bitxor", comment = "按位取异或")
    public Integer xor(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String a,
        @FunctionParamter(value = "String", comment = "代表要求值的字段名或常量") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        if (a == null || b == null) {
            return result;
        }
        result = aTem ^ aTem;
        return result;
    }

    /**
     * 按位取反，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitnot", comment = "按位取反", alias = "bitnot")
    public Integer not(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "取反的Integer常量") Integer a) {
        Integer result = null;
        if (a == null) {
            return result;
        }
        result = ~a;
        return result;
    }

    /**
     * 按位取反，输入和输出类型均为整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitnot", comment = "按位取反", alias = "bitnot")
    public Integer not(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "取反的字段或常量") String a) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        if (a == null) {
            return result;
        }
        result = ~aTem;
        return result;
    }

    /**
     * 运算符按位“与”操作，输入和输出类型均为INT整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitand", alias = "bitand", comment = "运算符按位“与”操作")
    public Integer and(IMessage message, FunctionContext context,
        @FunctionParamter(value = "Integer", comment = "按位取反的第一个常量值") Integer a,
        @FunctionParamter(value = "Integer", comment = "按位取反的第二个常量值") Integer b) {
        Integer result = null;
        if (a == null || b == null) {
            return result;
        }
        result = a & b;
        return result;
    }

    /**
     * 运算符按位“与”操作，输入和输出类型均为INT整型，且类型一致
     *
     * @param message
     * @param context
     * @param a
     * @param a
     * @return
     */
    @FunctionMethod(value = "bitAnd", alias = "bitAnd", comment = "运算符按位“与”操作")
    public Integer and(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "按位取反的第一个常量值") String a,
        @FunctionParamter(value = "String", comment = "按位取反的第二个常量值") String b) {
        Integer result = null;
        Integer aTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        Integer bTem = Integer.parseInt(FunctionUtils.getValueString(message, context, a));
        if (a == null || b == null) {
            return result;
        }
        result = aTem & bTem;
        return result;
    }
}
