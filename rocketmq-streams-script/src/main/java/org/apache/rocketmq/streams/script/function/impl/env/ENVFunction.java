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
package org.apache.rocketmq.streams.script.function.impl.env;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ENVUtile;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class ENVFunction {
    public static final String ORIG_MESSAGE = "inner_message";

    @FunctionMethod(value = "env", comment = "取得字段evnKey的值")
    public String doENV(IMessage message, AbstractContext context,
                        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String envKey) {
        envKey = FunctionUtils.getValueString(message, context, envKey);
        message.getMessageBody().getString(envKey);
        if (envKey.equals(ORIG_MESSAGE)) {
            return message.getMessageBody().toJSONString();
        }
        String value = ComponentCreator.getProperties().getProperty(envKey);
        if (value != null) {
            return value;
        }
        return ENVUtile.getENVParameter(value);
    }

}
