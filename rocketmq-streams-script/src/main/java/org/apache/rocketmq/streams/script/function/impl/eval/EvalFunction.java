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
package org.apache.rocketmq.streams.script.function.impl.eval;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class EvalFunction {
    protected ICache<String, FunctionScript> functionScriptICache = new SoftReferenceCache<>();

    public static boolean isFunction(String name) {
        return "exec_function".equals(name) || "eval_function".equals(name);
    }

    @FunctionMethod(value = "exec_function")
    public Object execFunction(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String script) {
        String scriptValue = FunctionUtils.getValueString(message, context, script);
        FunctionScript functionScript = functionScriptICache.get(scriptValue);
        if (functionScript == null) {
            functionScript = new FunctionScript(scriptValue);
            functionScript.init();
            functionScriptICache.put(scriptValue, functionScript);
        }
        if (functionScript.getScriptExpressions() == null || functionScript.getScriptExpressions().size() != 1) {
            throw new RuntimeException("can only support exec function, not scripts " + scriptValue);
        }
        IScriptExpression scriptExpression = functionScript.getScriptExpressions().get(0);
        return scriptExpression.executeExpression(message, context);
    }

    @FunctionMethod(value = "eval_function")
    public Object execFunction(@FunctionParamter(value = "string", comment = "代表字符串的字段名或常量") String scriptValue) {
        scriptValue = FunctionUtils.getConstant(scriptValue);
        FunctionScript functionScript = functionScriptICache.get(scriptValue);
        if (functionScript == null) {
            functionScript = new FunctionScript(scriptValue);
            functionScript.init();
            functionScriptICache.put(scriptValue, functionScript);
        }
        if (functionScript.getScriptExpressions() == null || functionScript.getScriptExpressions().size() != 1) {
            throw new RuntimeException("can only support exec function, not scripts " + scriptValue);
        }
        IScriptExpression scriptExpression = functionScript.getScriptExpressions().get(0);
        Message message = new Message(new JSONObject());
        FunctionContext context = new FunctionContext(message);
        return scriptExpression.executeExpression(message, context);
    }

    @FunctionMethod("udfname")
    public Object udf(IMessage message, FunctionContext context) {
        Object object = ScriptComponent.getInstance().getFunctionService().executeFunction(message, context,
            "execFunction", "你的脚本");
        return object;
    }

    public static void main(String[] args) {
        String value = "now()";

        Object object = ScriptComponent.getInstance().getFunctionService().directExecuteFunction("eval_function", "now()");
        System.out.println(object);
    }
}
