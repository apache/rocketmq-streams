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
package org.apache.rocketmq.streams.script.optimization;

import org.apache.rocketmq.streams.script.function.impl.condition.EqualsFunction;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;

public class EqualsOptimization extends AbstractFunctionOptimization {

    @Override
    public OptimizationScriptExpression optimize(IScriptExpression scriptExpression) {
        String functionName = scriptExpression.getFunctionName();
        ScriptExpression expression = (ScriptExpression)scriptExpression;
        String varName = getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(0));
        String regex = getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(1));
        return new OptimizationScriptExpression(varName, regex, expression.getNewFieldName(), expression, EqualsFunction.isNotEquals(functionName));
    }

    @Override
    protected boolean supportOptimization(IScriptExpression scriptExpression) {
        String functionName = scriptExpression.getFunctionName();
        boolean match = EqualsFunction.isEquals(functionName);
        if (match) {
            return true;
        }
        return EqualsFunction.isNotEquals(functionName);
    }
}
