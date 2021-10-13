package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;

public class MutilCaseWhenExpression extends AbstractWhenExpression {
    public MutilCaseWhenExpression(String namespace, String name) {
        super(namespace, name);
    }


    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        for(String key:varNames2GroupByVarCaseWhen.keySet()){
            executeGroupByVarCaseWhen(key,varNames2GroupByVarCaseWhen.get(key),message,context);
        }
        if(notCacheCaseWhenElement!=null){
            for(CaseWhenElement caseWhenElement:this.notCacheCaseWhenElement){
                caseWhenElement.executeDirectly(message,context);
            }
        }
        return null;
    }


    @Override protected boolean executeThenDirectly() {
        return true;
    }
}
