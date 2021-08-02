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
package org.apache.rocketmq.streams.script.function.impl.mock;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class MockFunction {

    @FunctionMethod("mock")
    public void mock(IMessage message, AbstractContext context, String key, String value) {
        key = FunctionUtils.getValueString(message, context, key);
        value = FunctionUtils.getValueString(message, context, value);
        if (StringUtil.isEmpty(key) || StringUtil.isEmpty(value)) {
            return;
        }
        ComponentCreator.getProperties().put(key, value);
    }
}
