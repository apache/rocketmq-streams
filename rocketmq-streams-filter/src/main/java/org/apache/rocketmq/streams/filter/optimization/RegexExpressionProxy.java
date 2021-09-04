package org.apache.rocketmq.streams.filter.optimization;

import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class RegexExpressionProxy extends AbstractExpressionProxy {
    public RegexExpressionProxy(Expression oriExpression, Rule rule) {
        super(oriExpression, rule);
    }


    @Override public boolean support(Expression oriExpression) {
        return RegexFunction.isRegex(oriExpression.getFunctionName()) && DataTypeUtil.isString(oriExpression.getDataType()) && oriExpression.getValue() != null;
    }
}
