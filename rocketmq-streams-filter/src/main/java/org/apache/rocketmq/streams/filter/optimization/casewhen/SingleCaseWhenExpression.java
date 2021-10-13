package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class SingleCaseWhenExpression extends AbstractWhenExpression {
    protected List<IScriptExpression> elseScriptExpressions;
    protected List<IScriptExpression> afterScriptExpressions;
    public SingleCaseWhenExpression(String namespace, String name,List<IScriptExpression> elseScriptExpressions,List<IScriptExpression> afterScriptExpressions) {
        super(namespace, name);
        this.elseScriptExpressions=elseScriptExpressions;
        this.afterScriptExpressions=afterScriptExpressions;
    }
    @Override public Object executeExpression(IMessage message, FunctionContext context) {
        int minIndex=100000;

        CaseWhenElement firstMatchCaseWhenElement=null;
        for(String key:varNames2GroupByVarCaseWhen.keySet()){
           List<CaseWhenElement> caseWhenElements= executeGroupByVarCaseWhen(key,varNames2GroupByVarCaseWhen.get(key),message,context);
            if(caseWhenElements!=null){
                for(CaseWhenElement caseWhenElement:caseWhenElements){
                    Integer index=this.caseWhenElementIndexMap.get(caseWhenElement);
                    if(index<minIndex){
                        minIndex=index;
                        firstMatchCaseWhenElement=caseWhenElement;
                    }
                }
            }
        }
        if(notCacheCaseWhenElement!=null){
            for(CaseWhenElement caseWhenElement:this.notCacheCaseWhenElement){
                boolean success=caseWhenElement.executeCase(message,context);
                if(success){
                    Integer index=this.caseWhenElementIndexMap.get(caseWhenElement);
                    if(index<minIndex){
                        minIndex=index;
                        firstMatchCaseWhenElement=caseWhenElement;
                    }
                }
            }
        }
        boolean isMatch=false;
        if(firstMatchCaseWhenElement!=null){
            firstMatchCaseWhenElement.executeThen(message,context);
            isMatch=true;
        }else {
            if(elseScriptExpressions!=null){
                for(IScriptExpression elseExpression:this.elseScriptExpressions){
                    elseExpression.executeExpression(message,context);
                }
                isMatch=true;
            }
        }
        if(isMatch&&afterScriptExpressions!=null){
            for(IScriptExpression scriptExpression:this.afterScriptExpressions){
                scriptExpression.executeExpression(message,context);
            }
        }
        return null;
    }


    @Override protected boolean executeThenDirectly() {
        return false;
    }
}
