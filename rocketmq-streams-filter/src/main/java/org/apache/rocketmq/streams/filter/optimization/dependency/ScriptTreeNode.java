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
package org.apache.rocketmq.streams.filter.optimization.dependency;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.RuleExpression;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.optimization.casewhen.AbstractWhenExpression;
import org.apache.rocketmq.streams.filter.optimization.casewhen.SingleCaseWhenExpression;
import org.apache.rocketmq.streams.filter.optimization.script.ScriptOptimization;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class ScriptTreeNode extends TreeNode<ScriptChainStage> {
    protected transient ScriptDependent scriptDependent;
    public ScriptTreeNode(ChainPipeline pipeline, ScriptChainStage stage,
        TreeNode parent) {
        super(pipeline, stage, parent);
        FunctionScript functionScript=(FunctionScript)stage.getScript();
        scriptDependent=new ScriptDependent(functionScript);
    }

    @Override public Set<String> traceaField(String varName, AtomicBoolean isBreak, List<IScriptExpression> depenentScripts) {
        return scriptDependent.traceaField(varName,isBreak,depenentScripts);
    }

    @Override public List<CommonExpression> traceDepenentToSource() {
        FunctionScript functionScript=(FunctionScript)stage.getScript();
        List<IScriptExpression> scriptExpressions=functionScript.getScriptExpressions();
        List<CommonExpression> commonExpressions=new ArrayList<>();
        for(IScriptExpression scriptExpression:scriptExpressions){
            if(CommonExpression.support(scriptExpression)){
                CommonExpression commonExpression=  new CommonExpression(scriptExpression);
                traceDepenentToSource(this,commonExpression,commonExpression.getVarName());
                commonExpressions.add(commonExpression);
            }else if(GroupScriptExpression.class.isInstance(scriptExpression)){
                GroupScriptExpression groupScriptExpression=(GroupScriptExpression)scriptExpression;
                List<CommonExpression> commonExpressionList=traceIfExpression(groupScriptExpression.getIfExpresssion());
                if(commonExpressionList!=null){
                    commonExpressions.addAll(commonExpressionList);
                }
                if(groupScriptExpression.getElseIfExpressions()!=null){
                    for(GroupScriptExpression subGroup:groupScriptExpression.getElseIfExpressions()){
                        commonExpressionList=traceIfExpression(subGroup.getIfExpresssion());
                        if(commonExpressionList!=null){
                            commonExpressions.addAll(commonExpressionList);
                        }
                    }
                }
            }else if(AbstractWhenExpression.class.isInstance(scriptExpression)){
                AbstractWhenExpression abstractWhenExpression=(AbstractWhenExpression)scriptExpression;
                List<IScriptExpression> scriptExpressionList=abstractWhenExpression.getIfExpressions();
                for(IScriptExpression ifExpression:scriptExpressionList){
                    List<CommonExpression> commonExpressionList=traceIfExpression(ifExpression);
                    if(commonExpressionList!=null){
                        commonExpressions.addAll(commonExpressionList);
                    }
                }
            }else if(scriptExpression instanceof ScriptOptimization.BlinkRuleV2Exprssion){
                ScriptOptimization.BlinkRuleV2Exprssion blinkRuleV2Exprssion=(ScriptOptimization.BlinkRuleV2Exprssion)scriptExpression;
                List<Expression> expressions=blinkRuleV2Exprssion.getExpressions();
                for(Expression expression:expressions){
                    List<CommonExpression> commonExpressionList=this.traceDepenentToSource(expression);
                    if(commonExpressionList!=null){
                        commonExpressions.addAll(commonExpressionList);
                    }
                }
            }
        }
        return commonExpressions;
    }

    protected List<CommonExpression> traceIfExpression(IScriptExpression expresssion) {

        if(expresssion.getFunctionName()!=null&&CaseFunction.isCaseFunction(expresssion.getFunctionName())){
            IScriptParamter scriptParamter=(IScriptParamter)expresssion.getScriptParamters().get(0);
            String expressionStr= IScriptOptimization.getParameterValue(scriptParamter);
            Rule rule= ExpressionBuilder.createRule("tmp","tmp",expressionStr);
            return traceDepenentToSource(rule);
        }
        if(RuleExpression.class.isInstance(expresssion)){
            RuleExpression ruleExpression=(RuleExpression)expresssion;
            return traceDepenentToSource(ruleExpression.getRule());
        }
        return null;
    }

}
