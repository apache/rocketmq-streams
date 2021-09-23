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
