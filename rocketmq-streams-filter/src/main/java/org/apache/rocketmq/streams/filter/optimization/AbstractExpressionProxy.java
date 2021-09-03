package org.apache.rocketmq.streams.filter.optimization;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.AbstractCacheFilter;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilterBulider;

import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public abstract class AbstractExpressionProxy extends Expression implements ICacheFilter<Expression> {
    protected Expression oriExpression;
    protected Rule rule;
    public AbstractExpressionProxy(Expression oriExpression,Rule rule){
        this.oriExpression=oriExpression;
        this.rule=rule;
        if(oriExpression!=null){
            oriExpression.copy(this);
        }

    }

    @Override public Boolean getExpressionValue(RuleContext context, Rule rule) {
        return execute(context.getMessage(),context);
    }

    @Override public boolean execute(IMessage message, AbstractContext context) {
        Boolean isMatch=(Boolean) context.get(this);
        if(isMatch!=null){
            return isMatch;
        }
        return executeOrigExpression(message,context);
    }

    public abstract boolean support(Expression oriExpression) ;

    private boolean executeOrigExpression(IMessage message, AbstractContext context) {
        RuleContext ruleContext = new RuleContext(message.getMessageBody(), rule);
        if (context != null) {
            context.syncSubContext(ruleContext);
        }
        return oriExpression.getExpressionValue(ruleContext,rule);
    }

    @Override public Expression getOriExpression() {
        return oriExpression;
    }


}
