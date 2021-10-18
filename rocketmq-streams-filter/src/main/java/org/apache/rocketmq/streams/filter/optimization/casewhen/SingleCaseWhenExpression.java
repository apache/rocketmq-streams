/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.filter.optimization.casewhen;

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
