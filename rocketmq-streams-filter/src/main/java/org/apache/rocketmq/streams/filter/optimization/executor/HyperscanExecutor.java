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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.HyperscanRegex;
import org.apache.rocketmq.streams.common.optimization.quicker.QuickFilterResult;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.optimization.script.ScriptOptimization;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

/**
 * cotains regexs :can execute by hyperscan and cache result
 * if hit cache then return result directly else execute by hyperscan
 */
public class HyperscanExecutor extends AbstractExecutor {

    //every script can not put same key, so need add namespace and name on key profix
    //every var can put different size value, but same key must keep same value size
    protected static BitSetCache cache ;//all script share the cache to can limit cache

    protected HyperscanRegex hyperscanRegex=new HyperscanRegex();
    protected Map<String, ScriptOptimization.IExpressionExecutor> allExpressions=new HashMap<>();//all expression contains like ,regex ,regex
    protected Map<String,Integer> expressionIndex=new HashMap<>();//expression index, create index by add expression order
    protected AtomicInteger indexCreator=new AtomicInteger(-1);//create expression index
    protected String varName;
    protected String namespace;
    protected String name;
    public HyperscanExecutor(String namespace, String name) {
        this.name=name;
        this.namespace=namespace;
    }

    public void compile(){
        if(cache==null){
            synchronized (HyperscanExecutor.class){
                if(cache==null){
                    cache=new BitSetCache(1000000);
                }
            }
        }

        hyperscanRegex.compile();
    }

    public void addExpression(Object expression){
        if(expression instanceof IScriptExpression){
            IScriptExpression scriptExpression=(IScriptExpression)expression;
            if(RegexFunction.isRegexFunction(scriptExpression.getFunctionName())){
                String varName= IScriptOptimization.getParameterValue((IScriptParamter) scriptExpression.getScriptParamters().get(0));
                String regex=IScriptOptimization.getParameterValue((IScriptParamter) scriptExpression.getScriptParamters().get(1));
                String key= MapKeyUtil.createKey(varName,scriptExpression.getFunctionName(),regex);
                allExpressions.put(key, new ScriptOptimization.IExpressionExecutor<IScriptExpression>() {
                    @Override public boolean execute(IMessage message, AbstractContext context) {
                        return regexFunction.match(message,(FunctionContext)context,varName,regex);
                    }
                });
                int index=indexCreator.incrementAndGet();
                expressionIndex.put(key,index);
                hyperscanRegex.addRegex(regex,index);
            }
        }else if(expression instanceof Expression){
            Expression filterExpression=(Expression)expression;
            String varName=filterExpression.getVarName();
            String functionName=filterExpression.getFunctionName();

            if(org.apache.rocketmq.streams.filter.function.expression.RegexFunction.isRegex(filterExpression.getFunctionName())){
                String regex=(String)filterExpression.getValue();
                String key=MapKeyUtil.createKey(varName,functionName,regex);
                allExpressions.put(key, new ScriptOptimization.IExpressionExecutor() {
                    @Override public boolean execute(IMessage message, AbstractContext context) {
                        return ExpressionBuilder.executeExecute(filterExpression,message,context);
                    }
                });
                int index=indexCreator.incrementAndGet();
                expressionIndex.put(key,index);
                hyperscanRegex.addRegex(regex,index);
            }
//
        }
    }

    @Override public QuickFilterResult execute(IMessage message, AbstractContext context) {
        if(indexCreator.get()==0){
            return null;
        }
        String varValue = context.getMessage().getMessageBody().getString(varName);
        BitSetCache.BitSet value = cache.get(MapKeyUtil.createKey(namespace,name,varValue));
        if (value == null) {
            value=new BitSetCache.BitSet(indexCreator.get()+1);
            Set<Integer> matchIndexs=this.hyperscanRegex.matchExpression(varValue);
            if(matchIndexs!=null){
                for(Integer index:matchIndexs){
                    value.set(index);
                }
            }
            cache.put(MapKeyUtil.createKey(namespace,name,varValue),value);
        }
        return new QuickFilterResult(value,expressionIndex){

            @Override public Boolean isMatch(IMessage msg, Object expression) {
                if(expression instanceof IScriptExpression){
                    if(RegexFunction.isRegexFunction(((IScriptExpression) expression).getFunctionName())){
                        IScriptExpression scriptExpression=(IScriptExpression)expression;
                        String varName=IScriptOptimization.getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(0));
                        String regex=IScriptOptimization.getParameterValue((IScriptParamter)scriptExpression.getScriptParamters().get(1));
                        return this.isMatch(varName,scriptExpression.getFunctionName(),regex);
                    }
                }else if(expression instanceof Expression){
                    Expression filterExpression=(Expression)expression;
                    if(LikeFunction.isLikeFunciton(filterExpression.getFunctionName())|| org.apache.rocketmq.streams.filter.function.expression.RegexFunction.isRegex(filterExpression.getFunctionName())){
                        return this.isMatch(filterExpression.getVarName(),filterExpression.getFunctionName(),(String)filterExpression.getValue());
                    }

                }
                return null;
            }
        };
    }

}
