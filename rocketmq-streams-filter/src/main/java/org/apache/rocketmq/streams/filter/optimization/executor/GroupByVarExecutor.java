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
package org.apache.rocketmq.streams.filter.optimization.executor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.FilterResultCache;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.optimization.result.GroupQuickFilterResult;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.function.impl.string.ToLowerFunction;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class GroupByVarExecutor extends AbstractExecutor implements IScriptOptimization.IOptimizationCompiler {
    protected String namespace;
    protected String name;
    protected List<IScriptExpression> scriptExpressions;
    //execute before regex execute
    protected List<IScriptExpression> beforeScriptExpressions=new ArrayList<>();


    protected transient Map<String,IScriptExpression> varName2Scripts=new HashMap<>();
    protected transient Map<String, List<String>> varName2DependentFields=new HashMap<>();
    public GroupByVarExecutor(String namespace,String name,List<IScriptExpression> expressions){
        this.name=name;
        this.namespace=namespace;
        this.scriptExpressions=expressions;
    }
    protected Map<String,HyperscanExecutor> varName2HyperscanExecutors=new HashMap<>();
    @Override public FilterResultCache execute(IMessage message, AbstractContext context) {
        if(CollectionUtil.isNotEmpty(beforeScriptExpressions)){
            for(IScriptExpression scriptExpression:this.beforeScriptExpressions){
                scriptExpression.executeExpression(message,(FunctionContext) context);
            }
        }
        Map<String,Integer> expression2QuickFilterResultIndexMap=new HashMap<>();
        List<FilterResultCache> results=new ArrayList<>();
        int index=0;
        for(HyperscanExecutor executor:varName2HyperscanExecutors.values()){
            FilterResultCache result=  executor.execute(message,context);
            if(result==null){
                continue;
            }

            results.add(result);
            Map<String,Integer> expressionIndexMap= result.getExpression2IndexMap();
            for(String key:expressionIndexMap.keySet()){
                expression2QuickFilterResultIndexMap.put(key,index);
            }
            index++;
        }
        return new GroupQuickFilterResult(expression2QuickFilterResultIndexMap,results);
    }

    public void addExpression(IScriptExpression expression){
        if(expression.getNewFieldNames()!=null&&expression.getNewFieldNames().size()==1){
            String newFieldName=expression.getNewFieldNames().iterator().next();
            varName2Scripts.put(newFieldName,expression);
            varName2DependentFields.put(newFieldName,expression.getDependentFields());
        }
        if(RegexFunction.isRegexFunction(expression.getFunctionName())){
            this.regist(IScriptOptimization.getParameterValue((IScriptParamter) expression.getScriptParamters().get(0)),expression);
        }else if(expression instanceof GroupScriptExpression){
            GroupScriptExpression groupScriptExpression=(GroupScriptExpression)expression;
            List<IScriptExpression> caseExpressions=new ArrayList<>();
            caseExpressions.add(groupScriptExpression.getIfExpresssion());
            if(groupScriptExpression.getElseIfExpressions()!=null){
                for(GroupScriptExpression elseIf:groupScriptExpression.getElseIfExpressions()){
                    caseExpressions.add(elseIf.getIfExpresssion());
                }
            }
            for(IScriptExpression scriptExpression:caseExpressions){
                if(CaseFunction.isCaseFunction(scriptExpression.getFunctionName())){
                    String expressionStr=IScriptOptimization.getParameterValue((IScriptParamter) scriptExpression.getScriptParamters().get(0));
                    List<Expression> expressions=new ArrayList<>();
                    ExpressionBuilder.createExpression("tmp","tmp",expressionStr,expressions,new ArrayList<>());
                    if(expressions.size()>0){
                        for(Expression simpleExpression:expressions){
                            this.regist(simpleExpression.getVarName(),simpleExpression);
                        }
                    }
                }
            }
        }
    }

    public List<IScriptExpression> compile() {
        for(IScriptExpression scriptExpression:this.scriptExpressions){
            this.addExpression(scriptExpression);
        }
        List<IScriptExpression> scriptExpressionList=new ArrayList<>();
        for(IScriptExpression scriptExpression:this.scriptExpressions){
            if(!this.beforeScriptExpressions.contains(scriptExpression)){
                scriptExpressionList.add(scriptExpression);
            }
        }
        Map<String, HyperscanExecutor> map=new HashMap<>();
        for(String key:varName2HyperscanExecutors.keySet()){
            HyperscanExecutor hyperscanExecutor=varName2HyperscanExecutors.get(key);
            if(hyperscanExecutor.allExpressions.size()>5){
                map.put(key,hyperscanExecutor);
                hyperscanExecutor.compile();
            }
        }
        this.varName2HyperscanExecutors=map;
        return scriptExpressionList;
    }

    @Override public List<IScriptExpression> getOptimizationExpressionList() {
        return scriptExpressions;
    }

    public void setScriptExpressions(
        List<IScriptExpression> scriptExpressions) {
        this.scriptExpressions = scriptExpressions;
    }


    protected void regist(String varName,Object expression){

        List<IScriptExpression> dependents= addBeforeExpression(varName);
        if(dependents==null){
            return;
        }
        Collections.sort(dependents, new Comparator<IScriptExpression>() {
            @Override public int compare(IScriptExpression o1, IScriptExpression o2) {
                List<String> varNames1= o1.getDependentFields();
                List<String> varNames2=o2.getDependentFields();
                for(String varName:varNames1){
                    if(o2.getNewFieldNames()!=null&&o2.getNewFieldNames().contains(varName)){
                        return 1;
                    }
                }
                for(String varName:varNames2){
                    if(o1.getNewFieldNames()!=null&&o1.getNewFieldNames().contains(varName)){
                        return -1;
                    }
                }
                return 0;
            }
        });
        for(IScriptExpression scriptExpression:dependents){
            if(!this.beforeScriptExpressions.contains(scriptExpression)){
                this.beforeScriptExpressions.add(scriptExpression);
            }
        }
        HyperscanExecutor hyperscanExecutor=varName2HyperscanExecutors.get(varName);
        if(hyperscanExecutor==null){
            hyperscanExecutor=new HyperscanExecutor(namespace,name);
            hyperscanExecutor.varName=varName;
            varName2HyperscanExecutors.put(varName,hyperscanExecutor);
        }
        hyperscanExecutor.addExpression(expression);
    }

    protected List<IScriptExpression> addBeforeExpression(String varName) {
       IScriptExpression scriptExpression= this.varName2Scripts.get(varName);
       if(scriptExpression==null){
           return new ArrayList<>();
       }
       if(!ToLowerFunction.isLowFunction(scriptExpression.getFunctionName())){
           return null;
       }
       List<IScriptExpression> list=new ArrayList<>();
       list.add(scriptExpression);
       //this.beforeScriptExpressions.add(scriptExpression);
       List<String> fields=scriptExpression.getDependentFields();
       if(fields!=null){
           for(String fieldName:fields){
               List<IScriptExpression> dependents=addBeforeExpression(fieldName);
                if(dependents==null){
                    return null;
                }else {
                    list.addAll(dependents);
                }
           }
       }
       return list;
    }

}
