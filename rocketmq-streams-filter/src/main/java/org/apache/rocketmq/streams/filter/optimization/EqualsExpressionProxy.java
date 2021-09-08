package org.apache.rocketmq.streams.filter.optimization;

import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class EqualsExpressionProxy extends AbstractExpressionProxy {
    public EqualsExpressionProxy(Expression oriExpression, Rule rule) {
        super(oriExpression, rule);
    }

    @Override public boolean support(Expression oriExpression) {
        return Equals.isEqualFunction(oriExpression.getFunctionName()) && DataTypeUtil.isString(oriExpression.getDataType()) && oriExpression.getValue() != null;
    }

    @Override public String getExpression() {
        return null;
    }
}
