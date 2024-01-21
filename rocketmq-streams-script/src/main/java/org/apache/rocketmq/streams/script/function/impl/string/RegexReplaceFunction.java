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
public class RegexReplaceFunction {

    private final static String NULL = "null";

    /**
     * 替换字段信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexreplace", comment = "根据正则匹配替换字段信息")
    public String regexreplace(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "被替换的字段代表列字段或常量值") String fieldName,
        @FunctionParamter(value = "string", comment = "要替换的字段代表列字段或常量值") String replaceStr,
        @FunctionParamter(value = "string", comment = "正则表达式") String pattern) {
        String index = null;
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern) || StringUtil.isEmpty(replaceStr)) {
            return index;
        }

        String strTem = StringUtil.groupRegex(ori, pattern, 1);

        return ori.replaceFirst(strTem, replaceStr);
    }

    /**
     * 替换正则匹配的位置信息
     *
     * @param message
     * @param context
     * @param fieldName
     * @param pattern
     * @return
     */
    @FunctionMethod(value = "regexreplace", comment = "根据正则匹配替换字段信息")
    public String regexreplace(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "被替换的字段代表列字段或常量值") String fieldName,
        @FunctionParamter(value = "string", comment = "要替换的字段代表列字段或常量值") String replaceStr,
        @FunctionParamter(value = "string", comment = "正则表达式") String pattern,
        @FunctionParamter(value = "long", comment = "正则匹配第几次数") Long occurrence) {
        String ori = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(ori) || StringUtil.isEmpty(pattern)) {
            return NULL;
        }

        String strTem = StringUtil.groupRegex(ori, pattern, 1);

        int i = 1;
        int position = 0;
        int index = 0;
        while (i <= occurrence) {
            index = ori.indexOf(strTem, position);
            position = index;
            i++;
        }
        return ori.substring(0, index) + (ori.substring(index).replaceFirst(strTem, replaceStr));
    }

}
