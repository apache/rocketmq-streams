package org.apache.rocketmq.streams.script.optimization.performance;

import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilterBulider;
import org.apache.rocketmq.streams.script.function.impl.condition.EqualsFunction;
import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class EqualsProxy extends SimpleScriptExpressionProxy {
    public EqualsProxy(IScriptExpression origExpression) {
        super(origExpression);
    }

    @Override
    public boolean supportOptimization(IScriptExpression scriptExpression) {
        String functionName = scriptExpression.getFunctionName();
        boolean match = EqualsFunction.isEquals(functionName);
        if (match) {
            return true;
        }
        return false;
    }

    @Override protected String getVarName() {
       return getParameterValue((IScriptParamter)this.origExpression.getScriptParamters().get(0));
    }

}
