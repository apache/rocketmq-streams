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
package org.apache.rocketmq.streams.script.function.impl.field;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class FieldFunction {

    @FunctionMethod(value = "field", alias = "get", comment = "获取字段值")
    public <T> T getFieldValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段的名称，不需要引号") String fieldName) {
        String name = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(name)) {
            name = fieldName;
        }
        return (T) message.getMessageBody().get(name);
    }

    @FunctionMethod(value = "map_field", alias = "get_map_field", comment = "获取字段值")
    public Object getMapFieldValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段的名称，不需要引号") String fieldName, String jsonField) {
        Map<String, Object> fieldValue = (Map<String, Object>) message.getMessageBody().get(FunctionUtils.getConstant(fieldName));
        if (fieldValue == null) {
            return null;
        }
        jsonField = FunctionUtils.getValueString(message, context, jsonField);
        return fieldValue.get(jsonField);
    }

    @FunctionMethod(value = "put_map_field", alias = "put_map_field", comment = "获取字段值")
    public Object putMapFieldValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段的名称，不需要引号") String mapFieldName, String fieldName, Object value) {
        Map<String, Object> fieldValue = (Map<String, Object>) message.getMessageBody().get(mapFieldName);
        if (fieldValue == null) {
            return null;
        }
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        return fieldValue.put(fieldName, value);
    }

    @FunctionMethod(value = "json_field", alias = "get_json_field", comment = "获取字段值")
    public Object getJsonFieldValue(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "字段的名称，不需要引号") String fieldName, String jsonField) {
        JSONObject fieldValue = message.getMessageBody().getJSONObject(fieldName);
        if (fieldValue == null) {
            return null;
        }
        jsonField = FunctionUtils.getConstant(jsonField);
        return fieldValue.get(jsonField);
    }

    @FunctionMethod(value = "char_length", alias = "len", comment = "求字段代码字符串或常量的长度")
    public int len(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName) {
        String value = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(value)) {
            return 0;
        }
        return value.length();
    }

    @FunctionMethod(value = "lower", alias = "low", comment = "把字符串转换称小写")
    public String lower(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String fieldName) {
        String value = FunctionUtils.getValueString(message, context, fieldName);
        if (StringUtil.isEmpty(value)) {
            return null;
        }
        return value.toLowerCase();
    }

    @FunctionMethod(value = "concat", comment = "连接字符串")
    public String concat(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String... fieldNames) {
        StringBuilder sb = new StringBuilder();
        for (String fieldName : fieldNames) {
            String value = FunctionUtils.getValueString(message, context, fieldName);
            if (value == null) {
                continue;
            }
            sb.append(value);
        }
        return sb.toString();
    }

    @FunctionMethod(value = "concat_ws", alias = "concat_sign", comment = "通过分隔符把字符串拼接在一起")
    public String concat_ws(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表分隔符的字段名或常量") String sign,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String... fieldNames) {
        sign = FunctionUtils.getValueString(message, context, sign);
        if (sign == null) {
            sign = ",";
        }
        List<String> values = Lists.newArrayList();
        for (String fieldName : fieldNames) {
            String value = FunctionUtils.getValueString(message, context, fieldName);
            if (value != null) {
                values.add(value);
            }
        }
        if (values.isEmpty()) {
            return null;
        }
        return String.join(sign, values);
    }

    @FunctionMethod(value = "lpad", comment = "在原串左边补n个pad字符串，如果原串长度小于len，则截断，使得整个字符串长度为len")
    public String lpad(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String ori,
        @FunctionParamter(value = "string", comment = "代表字符串长度字段名，数字或常量") String lenStr,
        @FunctionParamter(value = "string", comment = "代表补齐字符串的字段名或常量") String pad) {
        if (StringUtil.isEmpty(ori) || pad == null) {
            return null;
        }
        int len = FunctionUtils.getValueInteger(message, context, lenStr);
        String oriStr = FunctionUtils.getValueString(message, context, ori);
        String padStr = FunctionUtils.getValueString(message, context, pad);
        if (oriStr.length() > len) {
            return ori.substring(0, len + 1);
        }
        if (StringUtil.isEmpty(padStr)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        int size = oriStr.length();
        int startIndex = 0;
        for (int i = 0; i < len - size; i++) {
            sb.append(padStr.substring(startIndex, startIndex + 1));
            startIndex = startIndex + 1;
            if (startIndex >= pad.length() - 2) {
                startIndex = 0;
            }
        }
        sb.append(oriStr);
        return sb.toString();

    }

    @FunctionMethod(value = "rpad", comment = "在原串左边补n个pad字符串，如果原串长度小于len，则截断，使得整个字符串长度为len")
    public String rpad(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String ori,
        @FunctionParamter(value = "string", comment = "代表字符串长度字段名，数字或常量") String lenStr,
        @FunctionParamter(value = "string", comment = "代表补齐字符串的字段名或常量") String pad) {
        if (StringUtil.isEmpty(ori) || pad == null) {
            return null;
        }
        int len = Integer.valueOf(lenStr);
        String oriStr = FunctionUtils.getValueString(message, context, ori);
        String padStr = FunctionUtils.getValueString(message, context, pad);
        if (oriStr.length() > len) {
            return ori.substring(0, len + 1);
        }
        if (StringUtil.isEmpty(padStr)) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        sb.append(oriStr);
        int size = oriStr.length();
        int startIndex = 0;

        for (int i = 0; i < len - size; i++) {
            sb.append(padStr.substring(startIndex, startIndex + 1));
            startIndex = startIndex + 1;
            if (startIndex >= pad.length() - 2) {
                startIndex = 0;
            }
        }

        return sb.toString();

    }
}
