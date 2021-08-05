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

import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class AbstractFunctionOptimization implements IFunctionOptimization {

    @Override
    public boolean support(IScriptExpression scriptExpression) {
        if (!ScriptExpression.class.isInstance(scriptExpression)) {
            return false;
        }
        return supportOptimization(scriptExpression);
    }

    protected abstract boolean supportOptimization(IScriptExpression scriptExpression);

    protected String getParameterValue(IScriptParamter scriptParamter) {
        if (!ScriptParameter.class.isInstance(scriptParamter)) {
            return null;
        }
        ScriptParameter parameter = (ScriptParameter)scriptParamter;
        if (parameter.getRigthVarName() != null) {
            return null;
        }
        return FunctionUtils.getConstant(parameter.getLeftVarName());
    }
}
