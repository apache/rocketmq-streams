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
package org.apache.rocketmq.streams.script.function.impl.string;

import java.math.BigDecimal;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ToCharFunction {

    /**
     * 转化为字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "tostring", comment = "转化为字符串")
    public String tochar(IMessage message, FunctionContext context,
                         @FunctionParamter(comment = "带转换的字符串代表列名称或常量值", value = "string") String fieldName) {
        if (StringUtil.isEmpty(fieldName.toString())) {
            return null;
        }
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        return fieldName.toString();
    }

    /**
     * 转化为字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "tostring", comment = "转化为字符串")
    public String tochar(IMessage message, FunctionContext context,
                         @FunctionParamter(comment = "带转换的字符串代表Boolean类型的常量值", value = "boolean") Boolean fieldName) {
        if (StringUtil.isEmpty(fieldName.toString())) {
            return null;
        }
        return fieldName.toString();
    }

    /**
     * 转化为字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "tostring", comment = "转化为字符串")
    public String tochar(IMessage message, FunctionContext context,
                         @FunctionParamter(comment = "带转换的字符串代表Long类型的常量值", value = "long") Long fieldName) {
        if (StringUtil.isEmpty(fieldName.toString())) {
            return null;
        }
        return fieldName.toString();
    }

    /**
     * 转化为字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "tostring", comment = "转化为字符串")
    public String tochar(IMessage message, FunctionContext context,
                         @FunctionParamter(comment = "带转换的字符串代表BigDecimal类型的常量值", value = "bigdecimal") BigDecimal fieldName) {
        if (StringUtil.isEmpty(fieldName.toString())) {
            return null;
        }
        return fieldName.toString();
    }

    /**
     * 转化为字符串
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tochar", alias = "tostring", comment = "转化为字符串")
    public String tochar(IMessage message, FunctionContext context,
                         @FunctionParamter(comment = "带转换的字符串代表double类型的常量值", value = "double") Double fieldName) {
        if (StringUtil.isEmpty(fieldName.toString())) {
            return null;
        }
        return fieldName.toString();
    }
}
