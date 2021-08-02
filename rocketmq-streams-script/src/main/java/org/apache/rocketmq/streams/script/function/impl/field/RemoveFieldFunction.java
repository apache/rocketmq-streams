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

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class RemoveFieldFunction {

    @FunctionMethod(value = "rm", alias = "removeField", comment = "删除某个字段")
    public <T> T remove(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "字段名称，不需要引号") String fieldName) {
        Object value = message.getMessageBody().get(fieldName);
        message.getMessageBody().remove(fieldName);
        return (T)value;
    }

    @FunctionMethod(value = "rm", alias = "removeField", comment = "删除某个字段")
    public <T> T remove(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "字段名称，不需要引号") String... fieldNames) {
        if (fieldNames == null) {
            return null;
        }
        for (String fieldName : fieldNames) {
            String name = FunctionUtils.getValueString(message, context, fieldName);
            if (StringUtil.isEmpty(name)) {
                name = fieldName;
            }
            message.getMessageBody().remove(name);
        }
        return null;
    }

    @FunctionMethod(value = "delete", alias = "del", comment = "删除某个字段")
    public <T> T delete(IMessage message, FunctionContext context,
                        @FunctionParamter(value = "string", comment = "字段名称，不需要引号") String... fieldNames) {
        return remove(message, context, fieldNames);
    }
}
