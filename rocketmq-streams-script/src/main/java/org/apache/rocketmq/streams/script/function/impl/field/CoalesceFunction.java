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
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.List;

@Function
public class CoalesceFunction {

    @FunctionMethod("coalesce")
    public Object coalesce(IMessage message, FunctionContext context, String fieldName, String nullValue) {
        Object o = message.getMessageBody().get(fieldName);
        nullValue = FunctionUtils.getConstant(nullValue);
        if (o == null) {
            return nullValue;
        }
        return o;
    }

    public static void main(String[] args) {
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        //msg.put("age",null);
        List<IMessage> messages = scriptComponent.getService().executeScript(msg, "age=coalesce(age,'N/A');");
        for (IMessage message : messages) {
            System.out.println(message.getMessageBody());
        }
    }
}
