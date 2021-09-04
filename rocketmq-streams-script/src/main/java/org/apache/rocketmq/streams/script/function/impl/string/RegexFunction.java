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
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * Created by yuanxiaodong on 1/9/18.
 */

@Function
public class RegexFunction {

    private final static String NULL = "null";

    public static boolean isRegexFunction(String functionName) {
        if (functionName == null) {
            return false;
        }
        functionName = functionName.toLowerCase();
        if ("dipperregex".equals(functionName) || "reg".equals(functionName) || "regex".equals(functionName)) {
            return true;
        }
        return false;
    }

    public static boolean isNotRegexFunction(String functionName) {
        if (functionName == null) {
            return false;
        }
        functionName = functionName.toLowerCase();
        if ("!regex".equals(functionName)) {
            return true;
        }
        return false;
    }

    @FunctionMethod(value = "dipperregex", alias = "reg", comment = "通过正则表达式匹配字符串")
    public boolean match(IMessage message, FunctionContext context,
                         @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                         @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String pattern) {
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return false;
        }
        if (FunctionUtils.isConstant(pattern)) {
            pattern = FunctionUtils.getConstant(pattern);
        }
        Boolean cacheResult=context.getFilterCache(pattern,ori);
        if(cacheResult!=null){
            return cacheResult;
        }

        return StringUtil.matchRegex(ori, pattern);
    }

    @FunctionMethod(value = "regex", comment = "通过正则表达式匹配字符串")
    public boolean matchRegex(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                              @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String pattern) {
        return match(message, context, fieldName, pattern);
    }

    @FunctionMethod(value = "!regex", comment = "通过正则表达式匹配字符串")
    public boolean notMatchRegex(IMessage message, FunctionContext context,
                                 @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                                 @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String pattern) {
        return !match(message, context, fieldName, pattern);
    }

    @FunctionMethod(value = "regexGroup", alias = "regex_group", comment = "得到第一组匹配")
    public String regexGroup(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                             @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String regex) {
        return regexGroupIndex(message, context, fieldName, regex, "1");
    }

    @FunctionMethod(value = "regexGroupIndex", alias = "regex_group_index,REGEXP_EXTRACT", comment = "得到指定组匹配")
    public String regexGroupIndex(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                                  @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String pattern,
                                  @FunctionParamter(value = "string", comment = "指定分组数") String group) {

        if (StringUtil.isEmpty(fieldName) || StringUtil.isEmpty(pattern)) {
            return null;
        }
        String fieldValue = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isNotEmpty(fieldValue)) {
            int regexGroup = 1;
            if (StringUtil.isNotEmpty(group)) {
                regexGroup = Integer.valueOf(group);
            }
            pattern = FunctionUtils.getValueString(message, context, pattern);
            String newValue = StringUtil.groupRegex(fieldValue, pattern, regexGroup);

            return newValue;
        }
        return null;
    }

    @FunctionMethod(value = "regexReplace", alias = "REGEXP_REPLACE", comment = "指定第一组匹作为配扩展字段值")
    public String regexReplace(IMessage message, FunctionContext context, String fieldName, String regex, String replaceValue) {
        String value = FunctionUtils.getValueString(message, context, fieldName);
        if (value == null) {
            return null;
        }
        regex = FunctionUtils.getValueString(message, context, regex);
        replaceValue = FunctionUtils.getValueString(message, context, replaceValue);
        return value.replaceAll(regex, replaceValue);
    }

    @FunctionMethod(value = "extra", comment = "指定第一组匹作为配扩展字段值")
    public String extracteField(IMessage message, FunctionContext context,
                                @FunctionParamter(value = "string", comment = "新的字段值") String newFieldName,
                                @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                                @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String regex) {
        return extraFieldByGroup(message, context, newFieldName, fieldName, regex, "1");
    }

    @FunctionMethod(value = "extraByGroup", comment = "指定第几组匹作为配扩展字段值")
    public String extraFieldByGroup(IMessage message, FunctionContext context,
                                    @FunctionParamter(value = "string", comment = "新的字段值") String newFieldName,
                                    @FunctionParamter(value = "string", comment = "代表要匹配的列字段或常量值") String fieldName,
                                    @FunctionParamter(value = "string", comment = "正则表达式代表列字段或常量值") String regex,
                                    @FunctionParamter(value = "string", comment = "指定分组数") String group) {

        if (StringUtil.isEmpty(fieldName) || StringUtil.isEmpty(newFieldName) || StringUtil.isEmpty(regex)) {
            return null;
        }
        String fieldValue = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), fieldName);
        if (StringUtil.isNotEmpty(fieldValue)) {
            int regexGroup = 1;
            if (StringUtil.isNotEmpty(group)) {
                regexGroup = Integer.valueOf(group);
            }
            String newValue = StringUtil.groupRegex(fieldValue, regex, regexGroup);
            message.getMessageBody().put(newFieldName, newValue);
            return newValue;
        }
        return null;
    }
}
