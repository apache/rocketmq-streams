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
import java.net.URLDecoder;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class UrlDecodeFunction {

    /**
     * 将输入字符串从application/x-www-form-urlencoded MIME格式转为正常字符串
     *
     * @param message
     * @param context
     * @param str
     * @return
     */
    @FunctionMethod(value = "urldecode", comment = "将输入字符串从application/x-www-form-urlencoded MIME格式转为正常字符串")
    public String urldecode(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "带编码的字符串代表列名称或常量值") String str) {
        String result = null;
        String ori = FunctionUtils.getValueString(message, context, str);
        if (StringUtil.isEmpty(ori)) {
            return result;
        }
        try {
            result = URLDecoder.decode(ori, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

    /**
     * 将输入字符串从application/x-www-form-urlencoded MIME格式转为正常字符串
     *
     * @param message
     * @param context
     * @param fileName
     * @return
     */
    @FunctionMethod(value = "urldecode", comment = "将输入字符串从application/x-www-form-urlencoded MIME格式根据指定的编码转为正常字符串")
    public String urldecode(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "带编码的字符串代表列名称或常量值") String fileName,
        @FunctionParamter(value = "string", comment = "指定的编码格式") String encodeing) {
        String result = null;
        fileName = FunctionUtils.getValueString(message, context, fileName);
        encodeing = FunctionUtils.getValueString(message, context, encodeing);
        if (StringUtil.isEmpty(fileName)) {
            return result;
        }
        try {
            result = URLDecoder.decode(fileName, encodeing);
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }
        return result;
    }

}
