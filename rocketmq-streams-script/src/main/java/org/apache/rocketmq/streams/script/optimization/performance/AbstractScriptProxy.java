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

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class AbstractScriptProxy implements IScriptExpression {
    protected IScriptExpression origExpression;

    public AbstractScriptProxy(IScriptExpression origExpression) {
        this.origExpression = origExpression;
    }

    public abstract List<ICacheFilter> getCacheFilters();

    public abstract boolean supportOptimization(IScriptExpression scriptExpression);

    protected String getParameterValue(IScriptParamter scriptParamter) {
        if (!ScriptParameter.class.isInstance(scriptParamter)) {
            return null;
        }
        ScriptParameter parameter = (ScriptParameter) scriptParamter;
        if (parameter.getRigthVarName() != null) {
            return null;
        }
        return FunctionUtils.getConstant(parameter.getLeftVarName());
    }

    @Override public List<IScriptParamter> getScriptParamters() {
        return this.origExpression.getScriptParamters();
    }

    @Override public String getFunctionName() {
        return this.origExpression.getFunctionName();
    }

    @Override public String getExpressionDescription() {
        return this.origExpression.getExpressionDescription();
    }

    @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
        return this.origExpression.getScriptParamter(message, context);
    }

    public void setOrigExpression(IScriptExpression origExpression) {
        this.origExpression = origExpression;
    }

    @Override public String getScriptParameterStr() {
        return this.origExpression.getScriptParameterStr();
    }

    @Override public List<String> getDependentFields() {
        return this.origExpression.getDependentFields();
    }

    @Override public Set<String> getNewFieldNames() {
        return this.origExpression.getNewFieldNames();
    }
}
