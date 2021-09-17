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
package org.apache.rocketmq.streams.script.optimization.compile;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

/**
 * 根据运行时的信息，对ScriptExpression进行优化，使其能在多数场景更快
 */
public class CompileScriptExpression {
    protected ScriptExpression scriptExpression;
    protected boolean containsContext = true;//是否包含message，context前缀参数
    protected Object[] parameterTemplete;//参数的模版，在多数场景，参数是固定不变的，不需要频发求值
    protected FunctionConfigure functionConfigure;//对应的具体的函数执行对象
    protected Map<Integer, CompileParameter> notFixedFieldIndexs = new HashMap<>();//字段不是固定的字段索引

    protected boolean isSimpleNewFieldName = false;//返回值没有点分割符，可以直接赋值

    protected boolean supportCompileOptimization = true;//是否支持优化

    public CompileScriptExpression(ScriptExpression scriptExpression, FunctionConfigure functionConfigure) {
        this.scriptExpression = scriptExpression;
        this.functionConfigure = functionConfigure;
        /**
         * 不支持变参的优化
         */
        if (this.functionConfigure.isVariableParameter()) {
            supportCompileOptimization = false;
            return;
        }
        this.containsContext = functionConfigure.isStartWithContext();
        int len = scriptExpression.getParameters().size();
        if (this.containsContext) {
            len = len + 2;
        }
        Object[] parameterTemplete = new Object[len];
        int startIndex = 0;
        if (this.containsContext) {
            parameterTemplete[0] = new Message(new JSONObject());
            parameterTemplete[1] = new Context((IMessage)parameterTemplete[0]);
            startIndex = 2;
        }

        for (int i = startIndex; i < parameterTemplete.length; i++) {
            IScriptParamter scriptParamter = scriptExpression.getParameters().get(i - startIndex);
            CompileParameter compileParameter = new CompileParameter(scriptParamter, containsContext);
            if (!compileParameter.isFixedValue()) {
                notFixedFieldIndexs.put(i, compileParameter);
                parameterTemplete[i] = null;
            } else {
                parameterTemplete[i] = compileParameter.getValue(null, null);
            }
        }

        this.parameterTemplete = functionConfigure.getRealParameters(parameterTemplete);
        if (scriptExpression.getNewFieldName() != null && scriptExpression.getNewFieldName().indexOf(".") == -1) {
            this.isSimpleNewFieldName = true;
        }

    }

    public Object execute(IMessage message, FunctionContext context) {
        if (parameterTemplete == null || supportCompileOptimization == false) {
            return scriptExpression.execute(message, context);
        }
        Object[] parameters = new Object[parameterTemplete.length];
        int startIndex = 0;
        if (containsContext) {
            parameters[0] = message;
            parameters[1] = context;
            startIndex = 2;
        }

        for (int i = startIndex; i < parameters.length; i++) {
            parameters[i] = parameterTemplete[i];
        }
        if (notFixedFieldIndexs.size() > 0) {
            Iterator<Entry<Integer, CompileParameter>> it = notFixedFieldIndexs.entrySet().iterator();
            while (it.hasNext()) {
                Entry<Integer, CompileParameter> entry = it.next();
                Integer index = entry.getKey();
                CompileParameter compileParameter = entry.getValue();
                parameters[index] = functionConfigure.getRealValue(index,compileParameter.getValue(message, context));
            }
        }
        Object value = functionConfigure.directReflectExecute(parameters);
        if (isSimpleNewFieldName && value != null) {
            message.getMessageBody().put(scriptExpression.getNewFieldName(), value);
        } else {
            scriptExpression.setValue2Var(message, context, scriptExpression.getNewFieldName(), value);
        }
        return value;
    }
}
