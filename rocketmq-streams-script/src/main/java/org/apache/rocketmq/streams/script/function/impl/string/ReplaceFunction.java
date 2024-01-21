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

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ReplaceFunction {

    /**
     * 用NEW字符串替换a字符串中与OLD字符串完全重合的部分并返回a
     *
     * @param message
     * @param context
     * @param a
     * @param OLD
     * @param NEW
     * @return
     */
    @FunctionMethod(value = "replace", comment = "使用新字符串替换原字符串中与指定的字符串完全匹配的字符串")
    public String trim(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "原字符串代表列字段或常量名") String a,
        @FunctionParamter(value = "string", comment = "指定被替换的字符串") String OLD,
        @FunctionParamter(value = "string", comment = "用于替换的字符串") String NEW) {
        a = FunctionUtils.getValueString(message, context, a);
        OLD = FunctionUtils.getValueString(message, context, OLD);
        NEW = FunctionUtils.getValueString(message, context, NEW);
        if (StringUtil.isEmpty(a) || OLD == null || NEW == null) {
            return null;
        }
        return a.replaceAll(OLD, NEW);
    }

}
