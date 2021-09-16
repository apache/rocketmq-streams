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

package org.apache.rocketmq.streams.script.optimization.performance;

import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class RegexProxy extends SimpleScriptExpressionProxy {
    public RegexProxy(IScriptExpression origExpression) {
        super(origExpression);
    }

    @Override
    public boolean supportOptimization(IScriptExpression scriptExpression) {
        String functionName = scriptExpression.getFunctionName();
        boolean match = RegexFunction.isRegexFunction(functionName);
        if (match) {
            return true;
        }
        return false;
    }

    @Override protected String getVarName() {
        return getParameterValue((IScriptParamter) this.origExpression.getScriptParamters().get(0));
    }

}
