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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.function.impl.field.RemoveFieldFunction;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class CaseWhenBuilder {

    public static boolean canSupportMutilCaseWhen(GroupScriptExpression groupScriptExpression){
        if(CollectionUtil.isNotEmpty(groupScriptExpression.getElseIfExpressions())){
            return false;
        }
        if(CollectionUtil.isNotEmpty(groupScriptExpression.getElseExpressions())){
            return false;
        }
        return true;
    }

    public static boolean canSupportSingleCaseWhen(GroupScriptExpression groupScriptExpression){
//        if(CollectionUtil.isEmpty(groupScriptExpression.getElseIfExpressions())){
//            return false;
//        }
//        if(groupScriptExpression.getElseIfExpressions().size()<5){
//            return false;
//        }
//        return true;
        return false;
    }
    public static List<IScriptExpression> compile(String namespace,String name,List<IScriptExpression> expressions) {
        expressions=mergeGroupScriptExpression(expressions);
        expressions=groupGroupScriptExpression(namespace,name,expressions);
        return expressions;
    }

    protected static List<IScriptExpression> mergeGroupScriptExpression(List<IScriptExpression> expressions) {
        List<IScriptExpression> scriptExpressions=new ArrayList<>();
        boolean startGroup=false;
        GroupScriptExpression groupScriptExpression=null;
        List<IScriptExpression> beforeGroup=new ArrayList<>();
        Set<Integer> skpitIndex=new HashSet<>();
        for(int i=0;i<expressions.size();i++){
            if(skpitIndex.contains(i)){
                continue;
            }
            IScriptExpression scriptExpression=expressions.get(i);
           if(scriptExpression.getFunctionName()!=null&&(scriptExpression.getFunctionName().equals("start_if")|scriptExpression.getFunctionName().equals("start_if_true_false"))){
               startGroup=true;
           }else if(scriptExpression.getFunctionName()!=null&&(scriptExpression.getFunctionName().equals("end_if")|(scriptExpression.getFunctionName().equals("end_if_true_false")))){
               List<IScriptExpression> afterGroup=findAfterExpression(groupScriptExpression,expressions,i,skpitIndex);
               groupScriptExpression.setBeforeExpressions(beforeGroup);
               if(afterGroup!=null){
                   groupScriptExpression.setAfterExpressions(afterGroup);
               }
               scriptExpressions.add(groupScriptExpression);
               startGroup=false;
               beforeGroup=new ArrayList<>();
               groupScriptExpression=null;
           }else if(scriptExpression instanceof GroupScriptExpression){
               groupScriptExpression=(GroupScriptExpression)scriptExpression;
           }else if(startGroup){
               beforeGroup.add(scriptExpression);
           } else {
               scriptExpressions.add(scriptExpression);
           }
        }
        return scriptExpressions;
    }



    public static List<IScriptExpression> groupGroupScriptExpression(String namespace,String name,List<IScriptExpression> expressions) {
        List<IScriptExpression> scriptExpressions=new ArrayList<>();
        MutilCaseWhenExpression mutilCaseWhenExpression=null;
        Map<String,List<String>> newFieldNames2DependentFields=new HashMap<>();
        for(IScriptExpression scriptExpression:expressions){
            if(scriptExpression instanceof GroupScriptExpression){
                GroupScriptExpression groupScriptExpression=(GroupScriptExpression)scriptExpression;
                if(canSupportMutilCaseWhen(groupScriptExpression)){
                    if(mutilCaseWhenExpression==null){
                        mutilCaseWhenExpression=new MutilCaseWhenExpression(namespace,name);

                    }
                    Map<String,List<String>> map=new HashMap<>();
                    map.putAll(newFieldNames2DependentFields);
                    map.putAll(groupScriptExpression.getBeforeDependents());
                    CaseWhenElement caseWhenElement=new CaseWhenElement(groupScriptExpression);
                    Set<String> dependentFields=createDependentFields(caseWhenElement.getDependentFields(),map);
                    mutilCaseWhenExpression.registe(caseWhenElement,dependentFields);
                } else if (canSupportSingleCaseWhen(groupScriptExpression)){
                    Map<String,List<String>> map=new HashMap<>();
                    map.putAll(newFieldNames2DependentFields);
                    map.putAll(groupScriptExpression.getBeforeDependents());
                    SingleCaseWhenExpression singleCaseWhenExpression= registeSingleCaseWhen(namespace,name,groupScriptExpression,map);
                    scriptExpressions.add(singleCaseWhenExpression);
                }
                else {
                    scriptExpressions.add(groupScriptExpression);
                }
            }else {
                if(CollectionUtil.isNotEmpty(scriptExpression.getNewFieldNames())){
                    String newFieldName=scriptExpression.getNewFieldNames().iterator().next();
                    newFieldNames2DependentFields.put(newFieldName,scriptExpression.getDependentFields());

                }
                 scriptExpressions.add(scriptExpression);
            }
        }
        if(mutilCaseWhenExpression!=null){
            scriptExpressions.add(mutilCaseWhenExpression);
            mutilCaseWhenExpression.compile();
        }
        return scriptExpressions;
    }

    protected static SingleCaseWhenExpression registeSingleCaseWhen(String namespace,String name,GroupScriptExpression groupScriptExpression, Map<String, List<String>> map) {
        List<IScriptExpression> elseExpressions=groupScriptExpression.getElseExpressions();
        List<IScriptExpression> afterExpressions=groupScriptExpression.getAfterExpressions();
        SingleCaseWhenExpression singleCaseWhenExpression=new SingleCaseWhenExpression(namespace,name,elseExpressions,afterExpressions);

        List<GroupScriptExpression> elseifExpressions=groupScriptExpression.getElseIfExpressions();
        groupScriptExpression.setElseIfExpressions(null);
        groupScriptExpression.setAfterExpressions(null);
        CaseWhenElement caseWhenElement=new CaseWhenElement(groupScriptExpression,true);
        Set<String> dependentFields=createDependentFields(caseWhenElement.getDependentFields(),map);
        singleCaseWhenExpression.registe(caseWhenElement,dependentFields);

        groupScriptExpression.setElseIfExpressions(elseifExpressions);
        groupScriptExpression.setAfterExpressions(afterExpressions);
        for(GroupScriptExpression elseifGroupExpression:elseifExpressions){
            elseifGroupExpression.setBeforeExpressions(groupScriptExpression.getBeforeExpressions());
            caseWhenElement=new CaseWhenElement(elseifGroupExpression,true);
            dependentFields=createDependentFields(caseWhenElement.getDependentFields(),map);
            singleCaseWhenExpression.registe(caseWhenElement,dependentFields);
            elseifGroupExpression.setBeforeExpressions(null);
        }
        return singleCaseWhenExpression;
    }

    protected static Set<String> createDependentFields(Set<String> varNames, Map<String, List<String>> dependents) {

        Set<String> oriFieldNames=new HashSet<>();
        for(String varName:varNames){
            varName=FunctionUtils.getConstant(varName);
            if(!dependents.containsKey(varName)&&!(varName.startsWith("(")&&varName.endsWith(")"))){
                oriFieldNames.add(varName);
                continue;
            }
            List<String> dependentFields=null;
            if(varName.startsWith("(")&&varName.endsWith(")")){
                Rule rule= ExpressionBuilder.createRule("tmp","tmp",varName);
                dependentFields=new ArrayList<>(rule.getDependentFields());
            }else {
                dependentFields=dependents.get(varName);
            }
            Set<String> dependentFieldSet=new HashSet<>();
            dependentFieldSet.addAll(dependentFields);
            oriFieldNames.addAll(createDependentFields(dependentFieldSet,dependents));
        }
        return oriFieldNames;
    }


    /**
     * 找是否有对group赋值的语句，逻辑是先找group中的临时变量，找给临时变量赋值，且rm临时变量的语句
     * @param expression
     * @param expressions
     * @param
     * @param index
     * @return
     */
    private static List<IScriptExpression> findAfterExpression(GroupScriptExpression expression, List<IScriptExpression> expressions, int index, Set<Integer> skiptIndexs) {
        String tmpVarName=expression.getThenExpresssions().get(0).getNewFieldNames().iterator().next();
        IScriptExpression setValueExpression=null;
        IScriptExpression removeExpression=null;
        String setValueExpressionVar=null;
        for(int i=index+1;i<expressions.size();i++){
            IScriptExpression scriptExpression=expressions.get(i);
            if(StringUtil.isEmpty(scriptExpression.getFunctionName())){
                String value=IScriptOptimization.getParameterValue((IScriptParamter) scriptExpression.getScriptParamters().get(0));
                if(value.equals(tmpVarName)&&setValueExpression==null){
                    setValueExpression=scriptExpression;
                    setValueExpressionVar=value;
                    skiptIndexs.add(i);
                }
            }else if(RemoveFieldFunction.isFunction(scriptExpression.getFunctionName())&&setValueExpression!=null){
                String value=IScriptOptimization.getParameterValue((IScriptParamter) scriptExpression.getScriptParamters().get(0));
                if(setValueExpressionVar.equals(value)){
                    removeExpression=scriptExpression;
                    skiptIndexs.add(i);
                    break;
                }
            }
        }
        List<IScriptExpression> scriptExpressions=new ArrayList<>();
        if(setValueExpression!=null){
            scriptExpressions.add(setValueExpression);
        }
        if(removeExpression!=null){
            scriptExpressions.add(removeExpression);
        }
        return scriptExpressions;
    }
}
