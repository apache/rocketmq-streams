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
package org.apache.rocketmq.streams.script.function.impl.parser;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.LogParserUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class PaserBySplitFunction {
    private static final Log LOG = LogFactory.getLog(PaserBySplitFunction.class);
    private static final String CONST_MAP_KEY = "_const_flags";//存放常量替换的map

    @FunctionMethod(value = "paserByComma", comment = "根据英文逗号分割字符串")
    public JSONObject paserByComma(IMessage message, FunctionContext context,
                                   @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName) {
        String log = FunctionUtils.getValueString(message, context, fieldName);
        Map<String, String> flags = (Map<String, String>)context.get(CONST_MAP_KEY);
        if (flags == null) {
            flags = new HashMap<>();
        }
        return parseBySplit(message, context, log, fieldName, ",", flags);
    }

    /**
     * 根据char分割字符串，其中char通过ascii码转换过来，常用于使用不可见字符做分割
     *
     * @param message
     * @param context
     * @param asciiDec 十进制的ascii码
     * @return
     */
    @FunctionMethod(value = "paserBySign", comment = "根据char分割字符串，其中char通过ascii码转换过来，常用于使用不可见字符做分割")
    public JSONObject paserByAsciiSplit(IMessage message, FunctionContext context,
                                        @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName,
                                        @FunctionParamter(value = "string", comment = "代表分割符") String asciiDec) {
        char splitSign = (char)Integer.parseInt(asciiDec);
        String log = FunctionUtils.getValueString(message, context, fieldName);
        Map<String, String> flags = (Map<String, String>)context.get(CONST_MAP_KEY);
        if (flags == null) {
            flags = new HashMap<>();
        }
        return parseBySplit(message, context, log, fieldName, String.valueOf(splitSign), flags);
    }

    @FunctionMethod(value = "split", alias = "paserBySplit", comment = "通过分割符来进行日志解析")
    public JSONObject parseBySign(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "boolean", comment = "是否需要预先处理常量类型") boolean needConstants,
                                  @FunctionParamter(value = "boolean", comment = "是否需要预先处理带括号的数据") boolean needBacket,
                                  @FunctionParamter(value = "boolean", comment = "是否预先处理时间类型的数据") boolean needDate,
                                  @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName,
                                  @FunctionParamter(value = "string", comment = "代表分割符") String sign) {
        Map<String, String> flags = new HashMap<>();
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        String log = FunctionUtils.getValueString(message, context, fieldName);
        if (needConstants) {
            log = LogParserUtil.parseContants(log, flags);
        }
        if (needBacket) {
            log = LogParserUtil.parseBrackets(log, flags);
        }
        if (needDate) {
            log = LogParserUtil.parseDate(log, flags);
        }
        sign = FunctionUtils.getValueString(message, context, sign);
        return parseBySplit(message, context, log, fieldName, sign, flags);
    }

    @FunctionMethod(value = "split", alias = "paserBySplit", comment = "通过分割符来进行日志解析")
    public JSONObject parseBySign(IMessage message, FunctionContext context,
                                  @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName,
                                  @FunctionParamter(value = "string", comment = "代表分割符") String sign) {
        sign = FunctionUtils.getConstant(sign);
        String log = FunctionUtils.getValueString(message,context,fieldName);
        Map<String, String> flags = (Map<String, String>)context.get(CONST_MAP_KEY);
        if (flags == null) {
            flags = new HashMap<>();
        }
        return parseBySplit(message, context, log, fieldName, sign, flags);
    }

    /**
     * 按分隔符分割日志，根据名字做命名
     *
     * @param message
     * @param context
     * @param log
     * @param fieldName
     * @param sign
     * @param flags
     * @param names
     * @return
     */
    private static Map<String, String> signs = new HashMap();

    static {
        signs.put("|", "\\|");
    }

    /**
     * 通过分割符进行解析
     *
     * @param message
     * @param context
     * @param log       原始日志
     * @param fieldName 字段名称
     * @param sign      分割符号
     * @param flags     常量和原始值的映射
     * @return
     */
    private JSONObject parseBySplit(IMessage message, FunctionContext context, String log, String fieldName, String sign, Map<String, String> flags) {
        if (signs.containsKey(sign)) {
            sign = signs.get(sign);
        }
        String[] values = log.split(sign);
        Map jsonObject = new HashMap();
        for (int i = 0; i < values.length; i++) {
            String name = FunctionType.UDTF.getName() +i;
            String value = values[i];
            String tmp = flags.get(value);
            if (StringUtil.isNotEmpty(tmp)) {
                value = tmp;
            }
            jsonObject.put(name, value);
        }
        message.getMessageBody().remove(fieldName);
        message.getMessageBody().putAll(jsonObject);
        return message.getMessageBody();
    }
}
