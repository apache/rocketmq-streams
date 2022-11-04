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
public class SubStringIndexFunction {

    /**
     * 截取字符串a第count分隔符之前的字符串，如count为正则从左边开始截取，如果为负则从右边开始截取
     *
     * @param message
     * @param context
     * @param a
     * @param SEP
     * @param countStr
     * @return
     */
    @FunctionMethod(value = "substringindex", alias = "substr_index", comment = "截取字符串a第count分隔符之前的字符串，如count为正则从左边开始截取，如果为负则从右边开始截取")
    public String substringindex(IMessage message, FunctionContext context,
                                 @FunctionParamter(comment = "带拆分的字符串代表字列名称或常量值", value = "string") String a,
                                 @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") String SEP,
                                 @FunctionParamter(comment = "用于表示开始拆分的位置，正数表示从左边拆分，负数表示从右边拆分", value = "string") String countStr) {
        String result = null;
        a = FunctionUtils.getValueString(message, context, a);
        SEP = FunctionUtils.getValueString(message, context, SEP);
        int count = FunctionUtils.getValueInteger(message, context, countStr);
        if (StringUtil.isEmpty(a) || StringUtil.isEmpty(SEP)) {
            return result;
        }
        int i = 1;
        int position = 0;
        int index = -1;
        try {
            if (count > 0) {
                while (i <= count) {
                    index = a.indexOf(SEP, position + 1);
                    position = index;
                    i++;
                }
                result = a.substring(0, index);
            }
            if (count < 0) {
                while (i <= (-count)) {
                    index = a.lastIndexOf(SEP, position + 1);
                    position = index;
                    i++;
                }
                result = a.substring(index);
            }
        } catch (Exception e) {
            //			LOG.error("字符串截取异常，本次返回原始串，原始串为："+a,e);  打日志
            result = a;
        }
        return result;
    }


    @FunctionMethod(value = "substr", alias = "substring", comment = "截取从指定的索引startIndex开始扩展到索引endIndex处的字符")
    public String substringindex(IMessage message, FunctionContext context,
                                 @FunctionParamter(comment = "带拆分的字符串代表字列名称或常量值", value = "string") String oriMsg,
                                 @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") Integer startIndex,
                                 @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") Integer endIndex) {
        oriMsg = FunctionUtils.getValueString(message, context, oriMsg);
        int msgLength = oriMsg.length();
        if (startIndex >= msgLength) {
            return "";
        } else if (endIndex > msgLength) {
            endIndex = msgLength;
        }
        return oriMsg.substring(startIndex, endIndex);
    }

    @FunctionMethod(value = "substr", alias = "blink_substring", comment = "截取从指定的索引startIndex处开始扩展到此字符串的结尾")
    public String substringindex(IMessage message, FunctionContext context,
                                 @FunctionParamter(comment = "带拆分的字符串代表字列名称或常量值", value = "string") String oriMsg,
                                 @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") Integer startIndex) {
        oriMsg = FunctionUtils.getValueString(message, context, oriMsg);
        return oriMsg == null ? null : startIndex >= oriMsg.length() ? "" : oriMsg.substring(startIndex);
    }

    @FunctionMethod(value = "blink_substr", alias = "blink_substring", comment = "截取从指定的索引startIndex开始,长度为len的字符，index从1开始，需要做下处理")
    public String substringindexForBlink(IMessage message, FunctionContext context,
                                         @FunctionParamter(comment = "带拆分的字符串代表字列名称或常量值", value = "string") String oriMsgField,
                                         @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") String startIndex,
                                         @FunctionParamter(comment = "指定用于拆分原始字段的字符代表列名称或常量值", value = "string") String len) {
        String oriMsg = FunctionUtils.getValueString(message, context, oriMsgField);
        String index = FunctionUtils.getValueString(message, context, startIndex);
        String lengthStr = FunctionUtils.getValueString(message, context, len);
        Integer fromIndex = Integer.valueOf(index);
        Integer length = Integer.valueOf(lengthStr);
        int endIndex = -1;
        /**
         * 从1开始，需要-1，改成从0开始
         */
        if (fromIndex > 0) {
            fromIndex--;
            endIndex = fromIndex + length;
        } else {
            fromIndex = oriMsg.length() + fromIndex - length + 1;
            endIndex = fromIndex + length;
        }
        if (oriMsg!=null&&endIndex >= oriMsg.length()) {
            endIndex = oriMsg.length();
        }
        if(oriMsg==null){
            return null;
        }
        return oriMsg.substring(fromIndex, endIndex);
    }
}
