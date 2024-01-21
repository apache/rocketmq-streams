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
public class RegexSubStrFunction {

    /**
     * 从start_position位置开始，source中第nth_occurrence次匹配指定模式pattern的子串
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexsubstr", comment = "从start_position位置开始，source中第nth_occurrence次匹配指定模式pattern的子串")
    public String regexsubstr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表要处理的列名称或常量值") String fieldName,
        @FunctionParamter(value = "string", comment = "代表正则字段信息") String pattern) {
        String index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        pattern = FunctionUtils.getValueString(message, context, pattern);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori);
        if (m.find()) {
            strTem = m.group(1);
        }
        return strTem;
    }

    /**
     * 从start_position位置开始，source中第nth_occurrence次匹配指定模式pattern的子串
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexsubstr", comment = "获取正则匹配的位置信息")
    public String regexsubstr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表要处理的列名称或常量值") String fieldName,
        @FunctionParamter(value = "string", comment = "代表正则字段信息") String pattern,
        @FunctionParamter(value = "string", comment = "代表开始匹配的位置") Long position) {
        String index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return index;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);
        Matcher m = r.matcher(ori.substring(position.intValue()));
        if (m.find()) {
            strTem = m.group(1);
        }
        return strTem;
    }

    /**
     * 从start_position位置开始，source中第nth_occurrence次匹配指定模式pattern的子串
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexsubstr", comment = "获取正则匹配的位置信息")
    public String regexsubstr(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表要处理的列名称或常量值") String fieldName,
        @FunctionParamter(value = "string", comment = "代表正则字段信息") String pattern,
        @FunctionParamter(value = "string", comment = "代表开始匹配的位置") Long position,
        @FunctionParamter(value = "long", comment = "代表指定第几次出现") Long occurrence) {
        Long index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return null;
        }
        String strTem = "";
        Pattern r = Pattern.compile(pattern);

        int i = 1;
        while (i <= occurrence) {
            Matcher m = r.matcher(ori.substring(position.intValue()));
            if (m.find()) {
                strTem = m.group(1);
            }
            index = (long) ori.indexOf(strTem, position.intValue() + 1);
            position = index;
            i++;
        }
        return strTem;
    }

}
