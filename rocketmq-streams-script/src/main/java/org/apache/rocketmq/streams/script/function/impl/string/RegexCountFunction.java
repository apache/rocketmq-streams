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

import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.context.FunctionContext;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.rocketmq.streams.common.context.IMessage;

@Function
public class RegexCountFunction {

    /**
     * 计算source中从start_position开始，匹配指定模式pattern的子串的次数
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexcount", comment = "计算source中从start_position开始，匹配指定模式pattern的子串的次数")
    public Long regexcount(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式代表类字段或常量值") String pattern) {
        Long index = null;
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
        int startIndex = 0;
        int position = 0;
        while (true) {
            position = ori.substring(startIndex).indexOf(strTem);
            if (position > -1) {
                startIndex = position;
                index++;
            } else {
                break;
            }
        }
        return index;
    }

    /**
     * 计算source中从start_position开始，匹配指定模式pattern的子串的次数
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexcount", comment = "计算source中从start_position开始，匹配指定模式pattern的子串的次数")
    public Long regexcount(IMessage message, FunctionContext context,
                           @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                           @FunctionParamter(value = "string", comment = "正则表达式代表类字段或常量值") String pattern,
                           @FunctionParamter(value = "string", comment = "匹配开始的位置") Long position) {
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
        int startIndex = position.intValue();
        int positionTem = 0;
        while (true) {
            positionTem = ori.substring(startIndex).indexOf(strTem);
            if (position > -1) {
                startIndex = positionTem;
                index++;
            } else {
                break;
            }
        }
        index = (long)startIndex;
        return index;
    }
}
