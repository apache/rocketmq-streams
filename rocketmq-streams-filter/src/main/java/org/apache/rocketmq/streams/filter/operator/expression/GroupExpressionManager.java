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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.rocketmq.streams.common.optimization.RegexEngine;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;

/**
 * hyperscan和规则的结合，目前暂不启用。
 */
public class GroupExpressionManager {
    protected Rule rule;
    protected Map<String, RegexEngine> regexEngineMap = new HashMap<>();
    protected List<GroupExpression> groupExpressions = new ArrayList<>();

    public GroupExpressionManager(Rule rule) {
        this.rule = rule;

    }

    public void compile() {
        for (Expression expression : rule.getExpressionMap().values()) {
            if (SimpleExpression.class.isInstance(expression) && (RegexFunction.isRegex(expression.getFunctionName()))) {
                String varName = expression.getVarName();
                RegexEngine regexEngine = regexEngineMap.get(varName);
                if (regexEngine == null) {
                    regexEngine = new RegexEngine();
                    regexEngineMap.put(varName, regexEngine);
                }
                regexEngine.addRegex((String) expression.getValue(), expression.getName());
//                if(LikeFunction.isLikeFunciton(expression.getFunctionName())){
//                    String like=(String)expression.getValue();
//                    LikeRegex likeRegex=new LikeRegex(like);
//                    hyperscanRegex.addRegex(likeRegex.createRegex(),expression.getConfigureName());
//                }else if(RegexFunction.isRegex(expression.getFunctionName())){
//
//                }else {
//                    throw new RuntimeException("can not support other function name "+ expression.getFunctionName());
//                }

            }
        }
        for (RegexEngine theEngine : regexEngineMap.values()) {
            theEngine.compile();
        }
    }

    public void matchAndSetResult(RuleContext context) {
        Set<String> allRegexResult = new HashSet<>();
        JSONObject msg = context.getMessage().getMessageBody();
        Iterator<Entry<String, RegexEngine>> it = regexEngineMap.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, RegexEngine> entry = it.next();
            String varName = entry.getKey();
            String varValue = msg.getString(varName);
            RegexEngine regexEngine = entry.getValue();
            Set<String> expressionNames = regexEngine.matchExpression(varValue);
            allRegexResult.addAll(expressionNames);
        }
        for (GroupExpression groupExpression : groupExpressions) {
            groupExpression.setRegexResult(allRegexResult);
        }
    }

    public void addGroupExpression(GroupExpression groupExpression) {
        groupExpressions.add(groupExpression);
    }

}
