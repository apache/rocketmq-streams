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
package org.apache.rocketmq.streams.filter.operator.action.impl;

import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class CaseWhenAction extends Action {
    protected transient IScriptExpression scriptExpression;

    @Override public Object doAction(RuleContext context, Rule rule) {
        ScriptParameter scriptParamter= (ScriptParameter) scriptExpression.getScriptParamters().get(0);
        context.getMessage().getMessageBody().put(scriptParamter.getLeftVarName(), FunctionUtils.getConstant(scriptParamter.getRigthVarName()));
        return null;
    }

    @Override public boolean volidate(RuleContext context, Rule rule) {
        return true;
    }

    public IScriptExpression getScriptExpression() {
        return scriptExpression;
    }

    public void setScriptExpression(IScriptExpression scriptExpression) {
        this.scriptExpression = scriptExpression;
    }
}
