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
package org.apache.rocketmq.streams.script.function.impl.common;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Function
public class EchoFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(EchoFunction.class);

    @FunctionMethod(value = "echo", comment = "输出一个JSON字符串")
    public String echo(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String... fieldNames) {
        if (fieldNames == null) {
            LOGGER.info("echo message:" + message.getMessageBody());
            return message.getMessageBody().toJSONString();
        }
        JSONObject jsonObject = new JSONObject();
        JSONObject jsonMessage = message.getMessageBody();
        for (String fieldName : fieldNames) {
            jsonObject.put(fieldName, jsonMessage.getString(fieldName));
        }
        LOGGER.info("echo message:" + jsonObject.toJSONString());
        return jsonObject.toJSONString();
    }
}
