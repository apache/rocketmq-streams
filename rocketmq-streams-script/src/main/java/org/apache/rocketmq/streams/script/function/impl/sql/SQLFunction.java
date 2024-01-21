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
package org.apache.rocketmq.streams.script.function.impl.sql;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class SQLFunction {

    @FunctionMethod(value = "inSQL", alias = "create_inSQL", comment = "把字段对应的数组转化成sql中的in变量")
    public String createInSQL(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String msgFieldName,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String columnName,
        @FunctionParamter(value = "boolean", comment = "是否是String类型") boolean isString) {
        msgFieldName = FunctionUtils.getValueString(message, context, msgFieldName);
        columnName = FunctionUtils.getValueString(message, context, columnName);
        JSONArray jsonArray = message.getMessageBody().getJSONArray(msgFieldName);
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        for (int i = 0; i < jsonArray.size(); i++) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            String value = jsonObject.getString(columnName);
            if (isString) {
                value = "'" + value + "'";
            }
            stringBuilder.append(value);
        }
        return stringBuilder.toString();
    }
}
