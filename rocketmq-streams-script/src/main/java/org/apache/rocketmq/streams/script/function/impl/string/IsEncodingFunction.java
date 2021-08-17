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

import java.io.UnsupportedEncodingException;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class IsEncodingFunction {

    /**
     * 编码判断
     *
     * @param message
     * @param context
     * @param param
     * @param from
     * @param to
     * @return
     */
    @FunctionMethod(value = "isencoding", alias = "is_encoding", comment = "编码判断")
    public Boolean isencoding(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String param,
                              @FunctionParamter(value = "string", comment = "代表输入编码格式的字段名或常量") String from,
                              @FunctionParamter(value = "string", comment = "代表输出编码格式的字段名或常量") String to) {
        String ori = FunctionUtils.getValueString(message, context, param);
        from = FunctionUtils.getValueString(message, context, from);
        to = FunctionUtils.getValueString(message, context, to);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(from) || StringUtil.isEmpty(to)) {
            return null;
        }
        try {
            String dst = new String(ori.getBytes(from), to);
            return dst.equals(ori);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("不支持类型转换", e);
        }
    }
}
