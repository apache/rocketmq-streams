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
public class BlinkStringFunction {

    @FunctionMethod(value = "substring", alias = "substr", comment = "截取从index开始len长度的字符串,－1代表截取从index后的全部")
    public String doStrig(IMessage message, FunctionContext context,
                          @FunctionParamter(value = "string", comment = "字段名或常量") String filedName,
                          @FunctionParamter(value = "string", comment = "字段名，常量或数字") String startIndex,
                          @FunctionParamter(value = "string", comment = "字段名，常量或数字") String length) {
        String value = FunctionUtils.getValueString(message, context, filedName);
        int index = FunctionUtils.getValueInteger(message, context, startIndex);
        if (StringUtil.isEmpty(length)) {
            return value.substring(index);
        } else {
            int len = FunctionUtils.getValueInteger(message, context, length);
            if (len == -1) {
                len = value.length();
            }
            int toIndex = index + len;
            if (toIndex >= value.length()) {
                toIndex = value.length();
            }
            return value.substring(index, toIndex);
        }

    }

}
