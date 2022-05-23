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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class RegexParserFunction {
    private static final Log LOG = LogFactory.getLog(RegexParserFunction.class);

    @FunctionMethod(value = "paserByRegex", comment = "通过正则解析实例日志")
    public String paserByRegex(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "代表字符串的字段名") String fieldName,
                               @FunctionParamter(value = "string", comment = "正则表达式") String regex) {

        String log = FunctionUtils.getValueString(message, context, fieldName);
        regex=FunctionUtils.getConstant(regex);
        JSONObject jsonObject = parseLog(regex, fieldName, log);
        if (jsonObject == null) {
            context.breakExecute();
        }
        message.setMessageBody(jsonObject);
        return jsonObject.toJSONString();
    }

    /**
     * 解析实例日志
     *
     * @param regex    正则表达式
     * @param log      日志
     * @return regex和解析的字段和内容的对应关系
     */
    public static JSONObject parseLog(String regex, String fieldName, String log) {
        JSONObject result = new JSONObject();
        Pattern pattern = Pattern.compile(regex);
        Matcher matcher = pattern.matcher(log);
        if (!matcher.matches()) {
            LOG.error("parseLog error: log not match regex!" + regex + ":" + log);
            return null;
        }
        for (int i = 1; i <= matcher.groupCount(); i++) {
            String name = FunctionType.UDTF.getName()+i;
            result.put(name, matcher.group(i));
        }
        return result;
    }

}
