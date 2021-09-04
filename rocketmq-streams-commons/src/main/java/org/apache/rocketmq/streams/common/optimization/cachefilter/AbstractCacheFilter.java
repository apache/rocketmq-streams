package org.apache.rocketmq.streams.common.optimization.cachefilter;

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

/**
 * proxy a expression/function
 * @param <T>
 */
public abstract class AbstractCacheFilter<T> implements ICacheFilter<T> {
    protected T origExpression;
    protected String varName;
    public AbstractCacheFilter( String varName,T origExpression){
        this.origExpression=origExpression;
        this.varName=varName;
    }

    @Override public boolean execute(IMessage message, AbstractContext context) {
        Boolean isMatch=(Boolean) context.get(this);
        if(isMatch!=null){
            return isMatch;
        }
        return executeOrigExpression(message,context);
    }

    @Override public String getVarName() {
        return varName;
    }

    @Override public T getOriExpression() {
        return origExpression;
    }
}
