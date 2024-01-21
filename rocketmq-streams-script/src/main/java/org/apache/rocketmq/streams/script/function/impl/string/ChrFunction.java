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
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ChrFunction {

    /**
     * 根据ascii返回字符
     *
     * @param message
     * @param context
     * @param ascii
     * @return
     */
    @FunctionMethod(value = "chr", alias = "char", comment = "将给定ASCII码ascii转换成字符")
    public String chr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段名或常量") String ascii) {
        String number = null;
        int asciiTem = Integer.parseInt(FunctionUtils.getValueString(message, context, ascii));
        if (ascii == null || asciiTem < 0 || asciiTem > 255) {
            return number;
        }
        number = (char) asciiTem + "";
        return number;
    }

}
