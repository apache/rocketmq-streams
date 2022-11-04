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
package org.apache.rocketmq.streams.filter.optimization.script;

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.optimization.casewhen.IFExpressionOptimization;
import org.apache.rocketmq.streams.filter.optimization.dependency.BlinkRuleV2Expression;
import org.apache.rocketmq.streams.filter.optimization.executor.GroupByVarExecutor;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

@AutoService(IScriptOptimization.class)
public class ScriptOptimization implements IScriptOptimization {

    public static interface IExpressionExecutor<T> {
        boolean execute(IMessage message, AbstractContext context);
    }

    @Override public IOptimizationCompiler compile(List<IScriptExpression> expressions,
        IConfigurableIdentification configurableIdentification) {
        if (expressions != null) {
//            GroupByVarExecutor groupByVarExecutor = new GroupByVarExecutor(configurableIdentification.getNameSpace(), configurableIdentification.getConfigureName(), expressions);
//            expressions = blinkRuleV2ExpressionOptimizate(expressions, configurableIdentification);
//            groupByVarExecutor.setScriptExpressions(expressions);
//            return groupByVarExecutor;
            return new IFExpressionOptimization(configurableIdentification.getNameSpace(),configurableIdentification.getConfigureName(),expressions);
        }
        return null;
    }

    private List<IScriptExpression> blinkRuleV2ExpressionOptimizate(List<IScriptExpression> expressions,
        IConfigurableIdentification configurableIdentification) {
        List<IScriptExpression> expressionList = new ArrayList<>();
        for (IScriptExpression scriptExpression : expressions) {
            if (!FunctionScript.class.isInstance(configurableIdentification)) {
                expressionList.add(scriptExpression);
                continue;
            }
            FunctionScript functionScript = (FunctionScript) configurableIdentification;
            boolean isSupport = BlinkRuleV2Expression.isBlinkRuleV2Parser(scriptExpression, functionScript.getConfigurableService());
            if (!isSupport) {
                expressionList.add(scriptExpression);
                continue;
            }
            BlinkRuleV2Expression blinkRuleV2Expression = new BlinkRuleV2Expression("com.lyra.xs.udf.ext.sas_black_rule_v2", scriptExpression.getFunctionName());
            List<IScriptParamter> scriptParamters = scriptExpression.getScriptParamters();
            String[] varNames = new String[scriptParamters.size()];
            int i = 0;
            for (IScriptParamter scriptParamter : scriptParamters) {
                String var = IScriptOptimization.getParameterValue(scriptParamter);
                varNames[i] = var;
                i++;
            }
            BlinkRuleV2Expression.RuleSetGroup ruleSetGroup = blinkRuleV2Expression.getDependentFields(varNames);
            IScriptExpression ruleExpression = new BlinkRuleV2Exprssion(scriptExpression, ruleSetGroup);
            expressionList.add(ruleExpression);
        }
        return expressionList;
    }

    public static class BlinkRuleV2Exprssion implements IScriptExpression<Integer> {
        protected IScriptExpression ori;
        protected BlinkRuleV2Expression.RuleSetGroup group;
        protected Map<String, List<BlinkRuleV2Expression.RuleSet>> ruleSetByCoreVarNames;
        protected FingerprintCache fingerprintCache;

        public BlinkRuleV2Exprssion(IScriptExpression ori, BlinkRuleV2Expression.RuleSetGroup group) {
            this.ori = ori;
            this.group = group;
            ruleSetByCoreVarNames = group.optimizate();
            fingerprintCache = new FingerprintCache(1000000);
        }

        @Override public Integer executeExpression(IMessage message, FunctionContext context) {
            int ruleId = -1;
            for (String coreVarName : ruleSetByCoreVarNames.keySet()) {
                BitSetCache.BitSet bitSet = fingerprintCache.getLogFingerprint(coreVarName, message.getMessageBody().getString(coreVarName));
                if (bitSet != null) {
                    continue;
                }
                List<BlinkRuleV2Expression.RuleSet> ruleSets = ruleSetByCoreVarNames.get(coreVarName);
                for (BlinkRuleV2Expression.RuleSet ruleSet : ruleSets) {
                    boolean isMatch = ruleSet.execute(message, context);
                    if (isMatch) {
                        ruleId = ruleSet.getRuleId();
                        String varName = ori.getNewFieldNames().iterator().next();
                        message.getMessageBody().put(varName, ruleId);
                        return ruleId;
                    }
                }
                bitSet = new BitSetCache.BitSet(1);
                bitSet.set(0);
                fingerprintCache.addLogFingerprint(coreVarName, message.getMessageBody().getString(coreVarName), bitSet);
            }

            String varName = ori.getNewFieldNames().iterator().next();
            message.getMessageBody().put(varName, ruleId);
            return ruleId;
        }

        @Override public List<IScriptParamter> getScriptParamters() {
            return ori.getScriptParamters();
        }

        @Override public String getFunctionName() {
            return ori.getFunctionName();
        }

        @Override public String getExpressionDescription() {
            return ori.getExpressionDescription();
        }

        @Override public Object getScriptParamter(IMessage message, FunctionContext context) {
            return ori.getScriptParamter(message, context);
        }

        @Override public String getScriptParameterStr() {
            return ori.getScriptParameterStr();
        }

        @Override public List<String> getDependentFields() {
            return new ArrayList<>(group.getVarNames());
        }

        @Override public Set<String> getNewFieldNames() {
            return ori.getNewFieldNames();
        }

        public List<Expression> getExpressions() {
            List<Expression> expressions = new ArrayList<>();
            for (BlinkRuleV2Expression.RuleSet ruleSet : this.group.getRuleSets()) {
                for (Expression expression : ruleSet.getExpressions()) {
                    expressions.add(expression);
                }
            }
            return expressions;
        }
    }
}
