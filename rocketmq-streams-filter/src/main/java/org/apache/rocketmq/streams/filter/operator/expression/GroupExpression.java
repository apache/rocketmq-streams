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
package org.apache.rocketmq.streams.filter.operator.expression;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.optimization.RegexEngine;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.var.Var;

/**
 * group by var name from all expression
 */
public class GroupExpression extends Expression<List<Expression>> {
    protected Rule rule;
    protected String varName;
    protected FingerprintCache fingerprintCache;
    protected boolean isOrRelation = true;//是否是or关系
    protected Map<String, Boolean> expressionName2Result = new HashMap<>();//正则类表达式的名字和结果的映射
    protected Set<String> regexExpressionNameSet = new HashSet<>();//正则类表达式的名字
    protected RegexEngine regexEngine;
    protected List<Expression> notRegexExpression = new ArrayList<>();
    protected AtomicBoolean hasCompile = new AtomicBoolean(false);
    protected transient Var var;
    protected transient String fingerpringtNamespace;

    public GroupExpression(Rule rule, Var var, boolean isOrRelation) {
        this.rule = rule;
        this.var = var;
        this.varName = var.getVarName();
        this.isOrRelation = isOrRelation;
        this.setName(NameCreatorContext.get().createName("expression.group"));
        value = new ArrayList<>();
        this.setNameSpace(rule.getNameSpace());
        fingerprintCache = FingerprintCache.getInstance();
    }

    public static void main(String[] args) {
//        String content = "abdfdfd";
//        String regex = "ab.*fd";
//        System.out.println(StringUtil.matchRegex(content, regex));

        BitSetCache.BitSet bitset = new BitSetCache.BitSet(1);
//        bitset.set(0);
        System.out.println(bitset.getBytes().length);
    }

    public void compile() {
        if (!hasCompile.compareAndSet(false, true)) {
            return;
        }
        regexEngine = new RegexEngine();
        for (Expression expression : getValue()) {
            if (SimpleExpression.class.isInstance(expression) && (RegexFunction.isRegex(expression.getFunctionName()))) {
                regexEngine.addRegex((String) expression.getValue(), expression.getName());
            } else {
                notRegexExpression.add(expression);
            }
        }
        regexEngine.compile();

    }

    @Override
    public Boolean doMessage(IMessage message, AbstractContext context) {
        String varValue = context.getMessage().getMessageBody().getString(varName);
        BitSetCache.BitSet bitset = fingerprintCache.getLogFingerprint(getFingerprintNamespace(), varValue);
        if (bitset == null) {
            bitset = new BitSetCache.BitSet(1);
            Boolean isMatch = executeMatch(message, context);
            if (isMatch) {
                bitset.set(0);
            }
            fingerprintCache.addLogFingerprint(getFingerprintNamespace(), varValue, bitset);
        }
        return bitset.get(0);
    }

    @Override
    public String toString() {
        return "";
    }

    @Override
    public Set<String> getDependentFields(Map<String, Expression> expressionMap) {
        Set<String> set = new HashSet<>();
        set.add(varName);

        return set;
    }

    protected String getFingerprintNamespace() {
        if (fingerpringtNamespace == null) {
            return MapKeyUtil.createKey(getNameSpace(), rule.getName(), getName());
        }
        return fingerpringtNamespace;
    }

    private Boolean executeMatch(IMessage message, AbstractContext context) {
        compile();
        JSONObject msg = message.getMessageBody();
        String varValue = msg.getString(varName);
        Set<String> expressionNames = regexEngine.matchExpression(varValue);
        if (isOrRelation) {
            if (expressionNames.size() > 0) {
                return true;
            }
        } else {
            if (expressionNames.size() < regexEngine.size()) {
                return false;
            }
        }
        for (Expression expression : notRegexExpression) {
            boolean isMatch = expression.doMessage(message, context);
            ;
            if (isOrRelation) {
                if (isMatch) {
                    return true;
                }
            } else {
                if (!isMatch) {
                    return false;
                }
            }
        }
        if (isOrRelation) {
            return false;
        } else {
            return true;
        }
    }

    public void addExpressionName(Expression expression) {
        if (RegexFunction.isRegex(expression.getFunctionName())) {
            regexExpressionNameSet.add(expression.getName());
        }
//        if(LikeFunction.isLikeFunciton(expression.getFunctionName())){
//            regexExpressionNameSet.add(expression.getConfigureName());
//        }
        getValue().add(expression);
    }

    public void setRegexResult(Set<String> allRegexResult) {
        Iterator<String> it = regexExpressionNameSet.iterator();
        while (it.hasNext()) {
            String name = it.next();
            Boolean value = allRegexResult.contains(name);
            if (value == null) {
                value = false;
            }
            expressionName2Result.put(name, value);

        }
    }

    public int size() {
        return getValue().size();
    }

    public Collection<? extends String> getAllExpressionNames() {
        Set<String> names = new HashSet<>();
        for (Expression expression : getValue()) {
            names.add(expression.getName());
        }
        return names;
    }
}
