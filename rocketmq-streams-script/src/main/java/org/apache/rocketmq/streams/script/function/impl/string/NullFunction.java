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
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;

/**
 * 返回null 值
 */
@Function
public class NullFunction {

    @FunctionMethod(value = "null", alias = "isNull", comment = "判断某个字段是否是null")
    public boolean isNull(IMessage message, FunctionContext context, String fieldName) {
        if (fieldName == null) {
            return true;
        }
        Object object = message.getMessageBody().get(fieldName);
        return object == null;
    }

    @FunctionMethod(value = "!null", alias = "!isNull", comment = "判断某个字段是否是null")
    public boolean isNotNull(IMessage message, FunctionContext context, String fieldName) {
        return !isNull(message, context, fieldName);
    }
}
