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
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ExtraFieldFunction {
    @FunctionMethod(value = "rename", alias = "rn", comment = "修改字段名称")
    public String extra(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "代表新字段的名称，不需要引号") String newFieldName,
                        @FunctionParamter(value = "string", comment = "代表旧字段的字段，不需要引号") String oldFieldName) {
        Object value = FunctionUtils.getValue(message, context, oldFieldName);
        if (value == null) {
            return null;
        }
        String name = FunctionUtils.getValueString(message, context, newFieldName);
        if (StringUtil.isEmpty(name)) {
            name = newFieldName;
        }
        message.getMessageBody().remove(oldFieldName);
        message.getMessageBody().put(name, value);
        return value.toString();
    }

    /**
     * 把一个json字段展开到外层数据结构
     *
     * @param channelMessage
     * @param context
     * @param expandFieldName
     */
    @FunctionMethod(value = "expandField" ,alias = "expand_json", comment = "json字段展开到外层数据结")
    public void expandField(IMessage channelMessage, FunctionContext context,
                            @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String expandFieldName) {
        if (StringUtil.isEmpty(expandFieldName)) {
            return;
        }
        JSONObject jsonObject = channelMessage.getMessageBody().getJSONObject(expandFieldName);
        if (jsonObject == null) {
            return;
        }
        channelMessage.getMessageBody().remove(expandFieldName);
        channelMessage.getMessageBody().putAll(jsonObject);
    }
}
