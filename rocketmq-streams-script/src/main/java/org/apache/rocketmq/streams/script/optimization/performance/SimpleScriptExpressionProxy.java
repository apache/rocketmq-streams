package org.apache.rocketmq.streams.script.optimization.performance;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.cachefilter.AbstractCacheFilter;
import org.apache.rocketmq.streams.common.optimization.cachefilter.ICacheFilter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public abstract class SimpleScriptExpressionProxy extends AbstractScriptProxy {

    public SimpleScriptExpressionProxy(IScriptExpression origExpression) {
        super(origExpression);
    }
    protected List<ICacheFilter> optimizationExpressions=null;
    @Override
    public List<ICacheFilter> getCacheFilters() {
        IScriptExpression scriptExpression=this.origExpression;
        if(this.optimizationExpressions==null){
            synchronized (this){
                if(this.optimizationExpressions==null){
                    List<ICacheFilter> optimizationExpressions=new ArrayList<>();
                    optimizationExpressions.add(new AbstractCacheFilter(getVarName(),this.origExpression) {
                        @Override protected boolean executeOrigExpression(IMessage message, AbstractContext context) {
                            FunctionContext functionContext = new FunctionContext(message);
                            if (context != null) {
                                context.syncSubContext(functionContext);
                            }
                            Boolean isMatch=(Boolean)scriptExpression.executeExpression(message,functionContext);

                            if (context != null) {
                                context.syncContext(functionContext);
                            }
                            return isMatch;
                        }
                    });
                    this.optimizationExpressions=optimizationExpressions;
                }
            }
        }
        return this.optimizationExpressions;

    }


    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        Boolean value= this.optimizationExpressions.get(0).execute(message,context);
        if(this.origExpression.getNewFieldNames()!=null&&this.origExpression.getNewFieldNames().size()>0){
            message.getMessageBody().put(this.origExpression.getNewFieldNames().iterator().next(), value);
        }
        return value;
    }


    protected abstract String getVarName();
}
