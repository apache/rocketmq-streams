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

import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.context.FunctionContext;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;

import org.apache.rocketmq.streams.common.context.IMessage;

@Function
public class TransLateFunction {

    /**
     * 将str1出现在str2中的字符串替换成str3中的字符串
     *
     * @param message
     * @param context
     * @param str1
     * @param str2
     * @param str3
     * @return
     */

    @FunctionMethod(value = "translate", comment = "将原字符串中指定的字符串替换为新的字符串")
    public String translate(IMessage message, FunctionContext context,
                            @FunctionParamter(value = "string", comment = "原字符串代表列名称或常量值") String str1,
                            @FunctionParamter(value = "string", comment = "指定的要替换的字符串代表列名称或常量值") String str2,
                            @FunctionParamter(value = "string", comment = "新的字符串代表列名称或常量值") String str3) {
        str1 = FunctionUtils.getValueString(message, context, str1);
        str2 = FunctionUtils.getValueString(message, context, str2);
        str3 = FunctionUtils.getValueString(message, context, str3);
        if (StringUtil.isEmpty(str1) || StringUtil.isEmpty(str2) || StringUtil.isEmpty(str3)) {
            return null;
        }
        return str1.replaceAll(str2, str3);
    }

}
