package org.apache.rocketmq.streams.filter.optimization;

import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class LikeExpressionProxy extends AbstractExpressionProxy {
    public LikeExpressionProxy(Expression oriExpression, Rule rule) {
        super(oriExpression, rule);
    }

    @Override public boolean support(Expression oriExpression) {
        return LikeFunction.isLikeFunciton(oriExpression.getFunctionName()) && DataTypeUtil.isString(oriExpression.getDataType()) && oriExpression.getValue() != null;
    }

}
