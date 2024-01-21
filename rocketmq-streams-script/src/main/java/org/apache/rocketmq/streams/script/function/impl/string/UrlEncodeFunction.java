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
import java.net.URLEncoder;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class UrlEncodeFunction {

    public static void main(String[] args) throws UnsupportedEncodingException {
        StringBuilder result = new StringBuilder();
        String ss = "示例for url_encode:// (fdsf)";
        char temp[] = ss.toCharArray();
        for (char cha : temp) {
            if (cha > 64 && cha < 91) {
                result.append(cha);
            } else if (cha > 96 && cha < 123) {
                result.append(cha);
            } else if (cha == 46 || cha == 45 || cha == 42 || cha == 95) {
                result.append(cha);
            } else {
                result.append(URLEncoder.encode(cha + "", "UTF-8"));
            }
        }
        System.out.println(result.toString());

    }

    /**
     * 将输入字符串编码为application/x-www-form-urlencoded MIME格式
     *
     * @param message
     * @param context
     * @param str
     * @return
     */
    @FunctionMethod(value = "urlencode", comment = "将输入字符串编码为application/x-www-form-urlencoded MIME格式")
    public String urlencode(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "带编码的字符串代表列名称或常量值") String str) {
        String ori = FunctionUtils.getValueString(message, context, str);
        StringBuilder result = new StringBuilder();
        if (StringUtil.isEmpty(ori)) {
            return result.toString();
        }
        char temp[] = ori.toCharArray();
        for (char cha : temp) {
            if (cha > 64 && cha < 91) {
                result.append(cha);
            } else if (cha > 96 && cha < 123) {
                result.append(cha);
            } else if (cha == 46 || cha == 45 || cha == 42 || cha == 95) {
                result.append(cha);
            } else {
                try {
                    result.append(URLEncoder.encode(cha + "", "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }

    /**
     * 将输入字符串编码为application/x-www-form-urlencoded MIME格式
     *
     * @param message
     * @param context
     * @param str
     * @return
     */
    @FunctionMethod(value = "urlencode", comment = "将输入字符串根据指定的编码编码为application/x-www-form-urlencoded MIME格式")
    public String urlencode(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "带编码的字符串代表列名称或常量值") String str,
        @FunctionParamter(value = "string", comment = "指定的编码") String encodeing) {
        String ori = FunctionUtils.getValueString(message, context, str);
        StringBuilder result = new StringBuilder();
        if (StringUtil.isEmpty(ori)) {
            return result.toString();
        }
        char temp[] = ori.toCharArray();
        for (char cha : temp) {
            if (cha > 64 && cha < 91) {
                result.append(cha);
            } else if (cha > 96 && cha < 123) {
                result.append(cha);
            } else if (cha == 46 || cha == 45 || cha == 42 || cha == 95) {
                result.append(cha);
            } else {
                try {
                    result.append(URLEncoder.encode(cha + "", encodeing));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }
        }
        return result.toString();
    }

}
