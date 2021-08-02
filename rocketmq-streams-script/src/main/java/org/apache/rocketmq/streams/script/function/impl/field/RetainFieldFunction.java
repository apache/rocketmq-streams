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
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 需要保留的字段，一个日志中可能有很多字段， 但其中只有一部分需要保留存储到下一步的文件中， 这个函数可以设置最后需要保留哪些字段
 */

@Function
public class RetainFieldFunction {

    @FunctionMethod(value = "retainField", alias = "retain", comment = "需要保留的字段")
    public void retainField(IMessage message, FunctionContext context,
                            @FunctionParamter(value = "array", comment = "字段名，不需要引号") String... msgFieldNames) {
        Map<String, Object> map = new HashMap<>();
        for (String field : msgFieldNames) {
            if (FunctionUtils.isConstant(field)) {
                field = FunctionUtils.getConstant(field);
            }
            Object value = FunctionUtils.getValue(message, context, field);
            map.put(field, value);
        }
        message.getMessageBody().clear();
        message.getMessageBody().putAll(map);
    }

}
