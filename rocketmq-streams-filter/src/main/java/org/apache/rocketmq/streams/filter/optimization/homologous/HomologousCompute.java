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
package org.apache.rocketmq.streams.filter.optimization.homologous;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.IHomologousCalculate;
import org.apache.rocketmq.streams.common.optimization.LikeRegex;
import org.apache.rocketmq.streams.common.optimization.RegexEngine;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.optimization.dependency.CommonExpression;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.impl.string.RegexFunction;
import org.apache.rocketmq.streams.script.function.impl.string.ToLowerFunction;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

/**
 * Advance the regular and like expression of homologous rules and make global optimization
 */
public class HomologousCompute implements IHomologousCalculate {
    protected transient static String MUTIL_BLINK = "\\s+";
    protected transient List<CommonExpression> commonExpressions;//all expression
    /**
     * If a variable performs the ETL operation of merging multiple spaces into one space, it is marked as true
     * key：source var value：if merging multiple spaces into one space return true else return false
     */
    protected transient Map<String, Boolean> containEliminateSpaces = new HashMap<>();

    /**
     * For regular class expressions, use hyperscan to optimize
     */
    protected transient Map<String, RegexEngine> expressionRegexEngineMap = new HashMap<>();

    /**
     * Group by data source variable key：sourceVarName value：CommonExpression list
     */
    protected transient Map<String, SameVarExpressionGroup> groupBySourceVarName;
    /**
     * cache like regex key：sourceVarName
     */
    protected transient Map<String, LikeRegex> likeRegexMap = new HashMap<>();
    /**
     * cache of finger cache
     */
    protected transient FingerprintCache fingerprintCache;
    protected transient Long firstReceiveTime = null;
    protected transient AtomicLong COUNT = new AtomicLong(0);
    protected transient RegexFunction replaceFunction = new RegexFunction();
    private transient List<CommonExpression> notsupportExpressions = new ArrayList<>();

    public HomologousCompute(List<CommonExpression> commonExpressions) {
        this.commonExpressions = commonExpressions;
        Map<String, SameVarExpressionGroup> groupBySourceVarName = groupBySourceVarName(commonExpressions);
        this.groupBySourceVarName = groupBySourceVarName;
        registHyperscan(groupBySourceVarName);
        createExpressionIndexAndHomologousVar(groupBySourceVarName);
        fingerprintCache = FingerprintCache.getInstance();
    }

    @Override
    public void calculate(IMessage message, AbstractContext context) {
        printQPS();
        FunctionContext functionContext = new FunctionContext(message);
        if (context != null) {
            context.syncSubContext(functionContext);
        }
        Map<String, BitSetCache.BitSet> homologousResult = new HashMap<>();
        for (String sourceName : groupBySourceVarName.keySet()) {
            SameVarExpressionGroup sameVarExpressionGroup = groupBySourceVarName.get(sourceName);
            String content = message.getMessageBody().getString(sourceName);
            String origContent = content;
            BitSetCache.BitSet bitSet = fingerprintCache.getLogFingerprint(sourceName, origContent);
            if (bitSet != null) {
                continue;
            }
            if (bitSet == null) {
                bitSet = new BitSetCache.BitSet(sameVarExpressionGroup.size());
            }
            RegexEngine regexEngine = expressionRegexEngineMap.get(sourceName);
            if (regexEngine != null) {
                executeByHyperscan(message, functionContext, regexEngine, sourceName, content, bitSet);
            }
            if (sameVarExpressionGroup.others != null) {
                executeDirectly(message, functionContext, sameVarExpressionGroup.others, bitSet);
            }
            fingerprintCache.addLogFingerprint(sourceName, origContent, bitSet);
            homologousResult.put(sourceName, bitSet);
        }
        context.setHomologousResult(homologousResult);
    }

    /**
     * execute by hyperscan and set result to bitset
     *
     * @param message
     * @param functionContext
     * @param regexEngine
     * @param sourceVar
     * @param content
     * @param bitSet
     */
    protected void executeByHyperscan(IMessage message, FunctionContext functionContext, RegexEngine regexEngine,
        String sourceVar, String content,
        BitSetCache.BitSet bitSet) {
        if (this.containEliminateSpaces.containsKey(sourceVar)) {
            content = replaceFunction.regexReplace(message, functionContext, sourceVar, "'" + MUTIL_BLINK + "'", "' '");
        }
        Set<CommonExpression> matchResult = regexEngine.matchExpression(content);
        for (CommonExpression commonExpression : matchResult) {
            bitSet.set(commonExpression.getIndex());
        }
    }

    protected void executeDirectly(IMessage message, FunctionContext functionContext,
        List<CommonExpression> commonExpressionList, BitSetCache.BitSet bitSet) {
        for (CommonExpression commonExpression : commonExpressionList) {
            for (IScriptExpression scriptExpression : commonExpression.getScriptExpressions()) {
                scriptExpression.executeExpression(message, functionContext);
            }

            String regex = commonExpression.getValue();
            String content = message.getMessageBody().getString(commonExpression.getVarName());
            if (content == null || regex == null) {
                continue;
            }
            if (commonExpression.isRegex()) {
                boolean isMatch = StringUtil.matchRegex(content, regex);
                if (isMatch) {
                    bitSet.set(commonExpression.getIndex());
                }

            } else {

                LikeRegex likeRegex = this.likeRegexMap.get(regex);
                if (likeRegex == null) {
                    likeRegex = new LikeRegex(regex);
                    this.likeRegexMap.put(regex, likeRegex);
                }

                boolean isMatch = likeRegex.match(content);
                if (isMatch) {
                    bitSet.set(commonExpression.getIndex());
                }
            }
        }
    }

    /**
     * Check all expressions. If the expressions do not meet the conditions, filter them directly group by source var
     * name
     *
     * @param expressions
     * @return
     */
    protected Map<String, SameVarExpressionGroup> groupBySourceVarName(List<CommonExpression> expressions) {
        Map<String, SameVarExpressionGroup> map = new HashMap<>();

        for (CommonExpression commonExpression : expressions) {
            String sourceVarName = commonExpression.getSourceVarName();
            SameVarExpressionGroup sameVarExpressionGroup = map.get(sourceVarName);
            if (sameVarExpressionGroup == null) {
                sameVarExpressionGroup = new SameVarExpressionGroup();
                map.put(sourceVarName, sameVarExpressionGroup);
            }
            if (!isSupport(commonExpression)) {
                notsupportExpressions.add(commonExpression);
                continue;
            }

            sameVarExpressionGroup.add(commonExpression);
        }
        return map;
    }

    /**
     * Check all expressions. If the expressions do not meet the conditions, filter them directly
     *
     * @param expression
     * @return
     */
    protected boolean isSupport(CommonExpression expression) {
        if (expression.getValue() == null || expression.getVarName() == null || expression.getSourceVarName() == null) {
            return false;
        }
        if (expression.getScriptExpressions() == null || expression.getScriptExpressions().size() == 0) {
            return true;
        }
        for (IScriptExpression scriptExpression : expression.getScriptExpressions()) {
            String functionName = scriptExpression.getFunctionName();
            if (functionName == null || ToLowerFunction.isLowFunction(functionName) || functionName.toUpperCase().equals("REGEXP_REPLACE")) {
                if (functionName != null && functionName.toUpperCase().equals("REGEXP_REPLACE")) {
                    List<ScriptParameter> scriptParameters = scriptExpression.getScriptParamters();
                    if (scriptParameters.size() != 3) {
                        return false;
                    }
                    String regex = IScriptOptimization.getParameterValue(scriptParameters.get(1));
                    String replace = IScriptOptimization.getParameterValue(scriptParameters.get(2));
                    if (!(regex.toLowerCase().equals(MUTIL_BLINK) && replace.toLowerCase().equals(" "))) {
                        return false;
                    }
                    containEliminateSpaces.put(expression.getSourceVarName(), true);
                }

            } else {
                return false;
            }
        }
        return true;
    }

    /**
     * regist regex 2 hypescan ，only support regex Create a hyperscan with a sourceVar
     */
    private void registHyperscan(Map<String, SameVarExpressionGroup> groupBySourceVarName) {
        for (String sourceName : groupBySourceVarName.keySet()) {
            RegexEngine regexEngine = new RegexEngine();
            SameVarExpressionGroup sameVarExpressionGroup = groupBySourceVarName.get(sourceName);
            for (CommonExpression commonExpression : sameVarExpressionGroup.regexs) {
                regexEngine.addRegex(commonExpression.getValue(), commonExpression);
            }
            regexEngine.compile();
            expressionRegexEngineMap.put(sourceName, regexEngine);

        }
    }

    /**
     * Assign a number to each expression
     *
     * @param groupBySourceVarName
     */
    private void createExpressionIndexAndHomologousVar(Map<String, SameVarExpressionGroup> groupBySourceVarName) {
        for (String sourceName : groupBySourceVarName.keySet()) {
            SameVarExpressionGroup sameVarExpressionGroup = groupBySourceVarName.get(sourceName);
            sameVarExpressionGroup.createIndexAndHomologousVar();
        }
    }

    /**
     * print qps
     */
    private void printQPS() {
//        if(firstReceiveTime==null){
//            firstReceiveTime=System.currentTimeMillis();
//        }
//        long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
//        if (second == 0) {
//            second = 1;
//        }
//        double qps = COUNT.incrementAndGet() / second;
//
//
//        if (COUNT.get() % 1000 == 0) {
//            System.out.println(
//                " qps is " + qps + "。the count is " + COUNT.get() + ".the process time is " + second);
//        }

    }

    public Map<String, SameVarExpressionGroup> getGroupBySourceVarName() {
        return groupBySourceVarName;
    }

    protected class SameVarExpressionGroup {
        protected List<CommonExpression> regexs = new ArrayList<>();
        protected List<CommonExpression> others = new ArrayList<>();

        public void add(CommonExpression commonExpression) {
            if (commonExpression.isRegex()) {
                regexs.add(commonExpression);
            } else {
                others.add(commonExpression);
            }
        }

        public void createIndexAndHomologousVar() {
            int i = 0;
            for (CommonExpression commonExpression : regexs) {
                commonExpression.setIndex(i);
                commonExpression.addHomologousVarToExpression();
                i++;
            }
            for (CommonExpression commonExpression : others) {
                commonExpression.setIndex(i);
                commonExpression.addHomologousVarToExpression();
                i++;
            }
        }

        public int size() {
            return regexs.size() + others.size();
        }
    }
}
