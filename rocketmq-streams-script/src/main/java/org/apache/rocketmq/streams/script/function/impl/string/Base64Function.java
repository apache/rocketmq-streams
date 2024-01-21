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

import java.util.Base64;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class Base64Function {

    final Base64.Decoder decoder = Base64.getDecoder();
    final Base64.Encoder encoder = Base64.getEncoder();

    /**
     * 将BINARY类型数据转换成对应base64编码的字符串输出。
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "tobase64", comment = "把字节转换成base64")
    public String tobase64(IMessage message, FunctionContext context,
        @FunctionParamter(value = "array", comment = "字节数组") byte fieldName[]) {
        if (fieldName == null) {
            return null;
        }
        return encoder.encodeToString(fieldName);
    }

    /**
     * str VARCHAR 类型，base64编码的字符串。
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "frombase64", comment = "把代表字符串的字段或常量转换为字节数组")
    public byte[] frombase64(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段或常量") String fieldName) {
        byte[] bt = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        bt = decoder.decode(ori);
        return bt;
    }

    /**
     * str VARCHAR 类型，base64编码的字符串。
     *
     * @param message
     * @param context
     * @param fieldName
     * @return
     */
    @FunctionMethod(value = "decode16", comment = "把代表字符串的字段或常量转换为字节数组")
    public String decode16(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段或常量") String fieldName) {
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori)) {
            return null;
        }
        return StringUtil.decode16(ori);
    }
}