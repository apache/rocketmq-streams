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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class RegexInstrFunction {

    /**
     * 获取正则匹配的位置信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexinstr", comment = "获取正则匹配的信息所在的位置")
    public Long regexinstr(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列名称或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式") String pattern) {
        Long index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        index = (long)ori.indexOf(strTem);
        return index;
    }

    /**
     * 获取正则匹配的位置信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexinstr", comment = "从指定位置获取正则匹配的信息所在的位置")
    public Long regexinstr(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列名称或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式") String pattern,
                           @FunctionParamter(value = "long", comment = "指定的位置") Long position) {
        Long index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        index = (long)ori.indexOf(strTem, position.intValue());
        return index;
    }

    /**
     * 获取正则匹配的位置信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexinstr", comment = "从指定位置获取正则匹配的信息指定出现的次数所在的位置")
    public Long regexinstr(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列名称或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式") String pattern,
                           @FunctionParamter(value = "long", comment = "指定的位置") Long position,
                           @FunctionParamter(value = "long", comment = "指定正则出现的次数") Long occurrence) {
        Long index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        int i = 1;
        while (i <= occurrence) {
            index = (long)ori.indexOf(strTem, position.intValue() + 1);
            position = index;
            i++;
        }
        return index;
    }

    /**
     * 获取正则匹配的位置信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexinstr", comment = "从指定位置获取正则匹配的信息指定出现的次数所在的位置")
    public Long regexinstr(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列名称或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式") String pattern,
                           @FunctionParamter(value = "long", comment = "指定的位置") Long position,
                           @FunctionParamter(value = "long", comment = "指定正则出现的次数") Long occurrence,
                           @FunctionParamter(value = "long", comment = "0表示返回匹配的开始位置，1表示返回匹配的结束位置") Long returnOption) {
        Long index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        int i = 1;
        while (i <= occurrence) {
            index = (long)ori.indexOf(strTem, position.intValue() + 1);
            position = index;
            i++;
        }
        if (returnOption == 1) {
            return index + strTem.length();
        } else {
            return index;
        }

    }

    public static void main(String[] args) {
        String ori = "i love www.taobao.com";
        String pattern = "o[a-zA-Z]{1}";
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        System.out.println(strTem);
    }

}
