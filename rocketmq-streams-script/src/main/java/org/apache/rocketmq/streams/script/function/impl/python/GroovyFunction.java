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
package org.apache.rocketmq.streams.script.function.impl.python;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.operator.impl.GroovyScriptOperator;
import org.apache.rocketmq.streams.script.operator.impl.JPythonScriptOperator;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class GroovyFunction {
    private GroovyScriptOperator groovyScript = new GroovyScriptOperator();
    private ICache<String, GroovyScriptOperator> cache = new SoftReferenceCache<>();

    @FunctionMethod(value = "groovy", alias = "groovy", comment = "执行一个指定名称的python脚本")
    public JSONObject doPython(IMessage message, FunctionContext context,
                               @FunctionParamter(value = "string", comment = "python名称") String scriptValue) {
        scriptValue = FunctionUtils.getValueString(message, context, scriptValue);
        GroovyScriptOperator operator = cache.get(scriptValue);
        if (operator == null) {
            operator = new GroovyScriptOperator();
            operator.setValue(scriptValue);
            operator.init();
            cache.put(scriptValue, operator);
        }

        operator.doMessage(message, context);
        return message.getMessageBody();
    }
}
