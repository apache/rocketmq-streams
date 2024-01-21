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
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.LogParserUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class LogParserFunction {
    private static final String CONST_MAP_KEY = "_const_flags";//存放常量替换的map

    @FunctionMethod(value = "val", alias = "const", comment = "把数据做常量处理")
    public String doConstants(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName) {
        String log = FunctionUtils.getValueString(message, context, fieldName);
        Map<String, String> flags = (Map<String, String>) context.get(CONST_MAP_KEY);
        if (flags == null) {
            flags = new HashMap<>();
            context.put(CONST_MAP_KEY, flags);
        }
        log = LogParserUtil.parseContants(log, flags);
        message.getMessageBody().put(fieldName, log);
        return log;
    }

    @FunctionMethod(value = "backet", alias = "backet", comment = "把带括号的换成换位符")
    public String doBacket(IMessage message, FunctionContext context, String fieldName, String... signs) {
        String log = FunctionUtils.getValueString(message, context, fieldName);
        Map<String, String> flags = (Map<String, String>) context.get(CONST_MAP_KEY);
        if (flags == null) {
            flags = new HashMap<>();
            context.put(CONST_MAP_KEY, flags);
        }
        if (signs == null) {
            log = LogParserUtil.parseBrackets(log, flags);
        } else {
            String[] signArray = new String[signs.length];
            int i = 0;
            for (String sign : signs) {
                sign = FunctionUtils.getValueString(message, context, sign);
                signArray[i] = sign;
                i++;
            }
            log = LogParserUtil.parseBrackets(log, flags, signArray);
        }
        message.getMessageBody().put(fieldName, log);
        return log;
    }

    @FunctionMethod(value = "parse_express", alias = "express", comment = "通过分割符来进行日志解析")
    public JSONObject parseExpression(IMessage message, FunctionContext context,
        @FunctionParamter(value = "String", comment = "代表字符串的字段名或常量") String fieldName,
        @FunctionParamter(value = "String", comment = "代表分隔符的字段名或常量") String sign) {
        sign = FunctionUtils.getValueString(message, context, sign);
        String log = FunctionUtils.getValueString(message, context, fieldName);
        Map<String, String> result = LogParserUtil.parseExpression(log, sign);
        message.getMessageBody().remove(fieldName);
        message.getMessageBody().putAll(result);
        return message.getMessageBody();
    }

}
