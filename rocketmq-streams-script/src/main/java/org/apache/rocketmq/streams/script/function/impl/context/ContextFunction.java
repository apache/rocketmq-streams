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
package org.apache.rocketmq.streams.script.function.impl.context;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;

@Function
public class ContextFunction {

    public static final String INNER_MESSAGE = "inner_message";

    @FunctionMethod(value = "close_split_mode", alias = "closeSplitModel")
    public Boolean closeSplitMode(IMessage message, FunctionContext context) {
        context.closeSplitMode(message);
        return true;
    }

    @FunctionMethod(value = "copy_msg",alias = "orig_msg")
    public JSONObject joinInnerMessage(IMessage message, FunctionContext context) {
        return message.getMessageBody();
    }
}
