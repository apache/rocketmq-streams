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
package org.apache.rocketmq.streams.filter.function.script;

import com.google.auto.service.AutoService;
import java.util.Set;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.RuleExpression;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.IGroupScriptOptimization;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@AutoService(IGroupScriptOptimization.class)
public class GroupScriptOptimization implements IGroupScriptOptimization {



    @Override public Set<String> getDependentFields(IScriptExpression scriptExpression) {
        if (RuleExpression.class.isInstance(scriptExpression)) {
            return ((RuleExpression) scriptExpression).getRule().getDependentFields();
        }
        String expressionStr = FunctionUtils.getConstant(((IScriptParamter) scriptExpression.getScriptParamters().get(0)).getScriptParameterStr());
        Rule rule = ExpressionBuilder.createRule("tmp", "tmp", expressionStr);
        return rule.getDependentFields();
    }

    @Override public boolean isCaseFunction(IScriptExpression scriptExpression) {
        if (RuleExpression.class.isInstance(scriptExpression)) {
            return true;
        }
        return CaseFunction.isCaseFunction(scriptExpression.getFunctionName());
    }

    @Override public void compile(GroupScriptExpression groupScriptExpression) {
        IScriptExpression ifScriptExpression=groupScriptExpression.getIfExpresssion();
        if(CaseFunction.isCaseFunction(ifScriptExpression.getFunctionName())){
            if(ifScriptExpression.getScriptParamters()!=null&&ifScriptExpression.getScriptParamters().size()==1){
                IScriptParamter scriptParamter=(IScriptParamter)ifScriptExpression.getScriptParamters().get(0);
                if(ScriptParameter.class.isInstance(scriptParamter)){
                    ScriptParameter valueParameter=(ScriptParameter)scriptParamter;
                    if(valueParameter.getFunctionName()==null&&valueParameter.getRigthVarName()==null&&valueParameter.getNewFieldNames()==null){
                        if(valueParameter.getLeftVarName().startsWith("(")&&valueParameter.getLeftVarName().endsWith(")")){
                            String expressionStr = FunctionUtils.getConstant(valueParameter.getLeftVarName());
                            Rule rule = ExpressionBuilder.createRule("tmp", "tmp", expressionStr);
                            rule.optimize();
                            groupScriptExpression.setRule(rule);
                        }else {
                            groupScriptExpression.setBoolVar(valueParameter.getLeftVarName());
                        }
                    }
                }
            }
           for(GroupScriptExpression group: groupScriptExpression.getElseIfExpressions()){
               compile(group);
           }

        }
    }
}
