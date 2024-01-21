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
public class HashCodeFunction {

    /**
     * 返回字符串的hashCode()的绝对值。
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "hashcode", comment = "获取字符串的hashcode")
    public String hashcode(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String fieldName) {
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori)) {
            return null;
        }
        return Math.abs(fieldName.hashCode()) + "";
    }

}