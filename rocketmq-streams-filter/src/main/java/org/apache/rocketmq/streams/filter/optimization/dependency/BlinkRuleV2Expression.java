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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.udf.UDFScript;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * create by udf
 */
public class BlinkRuleV2Expression {
    protected String className;
    protected Map<String, List<BlinkRule>> tmpWhiteListOnline = new HashMap();
    protected Map<String, List<BlinkRule>> tmpWhiteListOb = new HashMap();
    protected String functionName;

    public BlinkRuleV2Expression() {
    }

    public BlinkRuleV2Expression(String fullClassName, String functionName) {
        this.className = fullClassName;
        this.functionName = functionName;
        parserExpressions();
    }

    private transient AtomicInteger COUNT = new AtomicInteger(0);

    public RuleSetGroup getDependentFields(String[] varNames) {
        RuleSetGroup ruleSetGroup = new RuleSetGroup();
        String modId = FunctionUtils.getConstant(varNames[0]);
        String isOnline = FunctionUtils.getConstant(varNames[1]);
        Map<String, List<BlinkRule>> current = tmpWhiteListOnline;
        if ("1".equals(isOnline)) {
            current = this.tmpWhiteListOb;
        }
        String[] kv = new String[varNames.length - 2];
        for (int i = 2; i < kv.length; i++) {
            kv[i - 2] = FunctionUtils.getConstant(varNames[i]);
        }
        JSONObject msg = createMsg(kv);
        List<BlinkRule> blinkRules = current.get(modId);
        Set<String> dependentVarNames = new HashSet<>();
        for (BlinkRule blinkRule : blinkRules) {
            List<Expression> when = blinkRule.createExpression(msg);
            if (CollectionUtil.isNotEmpty(when)) {
                dependentVarNames.addAll(blinkRule.getAllVars());
                RuleSet ruleSet = new RuleSet(when, blinkRule.getAllVars(), blinkRule.ruleId);
                ruleSetGroup.addRuleSet(ruleSet);
            }

        }
        return ruleSetGroup;
    }

    public static class RuleSetGroup {
        protected List<RuleSet> ruleSets = new ArrayList<>();
        protected Set<String> varNames = new HashSet<>();
        protected Map<String, Integer> varName2Count = new HashMap<>();

        public void addRuleSet(RuleSet ruleSet) {
            if (ruleSet == null || ruleSet.expressions == null) {
                return;
            }
            ruleSets.add(ruleSet);
            this.varNames.addAll(ruleSet.varNames);
            for (String varName : ruleSet.varNames) {
                Integer count = varName2Count.get(varName);
                if (count == null) {
                    count = 0;
                }
                count++;
                varName2Count.put(varName, count);
            }

        }

        public List<RuleSet> getRuleSets() {
            return ruleSets;
        }

        public Map<String, List<RuleSet>> optimizate() {
            List<Map.Entry<String, Integer>> list = new ArrayList<>(varName2Count.entrySet());
            Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
                @Override public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return (o2.getValue() - o1.getValue());
                }
            });
            int count = list.size();
            List<String> varNames = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Map.Entry<String, Integer> entry = list.get(i);
                String varName = entry.getKey();
                varNames.add(varName);
            }
            Map<String, List<RuleSet>> stringListMap = new HashMap<>();
            for (RuleSet ruleSet : this.ruleSets) {
                for (String varName : varNames) {
                    if (ruleSet.varNames.contains(varName)) {
                        List<RuleSet> ruleSetList = stringListMap.get(varName);
                        if (ruleSetList == null) {
                            ruleSetList = new ArrayList<>();
                            stringListMap.put(varName, ruleSetList);
                        }
                        ruleSetList.add(ruleSet);
                        break;
                    }

                }
            }
            return stringListMap;
        }

        public Set<String> getVarNames() {
            return varNames;
        }
    }

    public static class RuleSet {
        protected List<Expression> expressions;
        protected Collection<String> varNames;
        protected int ruleId;
        protected Rule rule;

        public RuleSet(List<Expression> expressions, Collection<String> varNames, int ruleId) {
            this.expressions = expressions;
            this.varNames = varNames;
            this.ruleId = ruleId;
            this.rule = createRuleExpression();
            this.rule.optimize();
        }

        public boolean execute(IMessage message, AbstractContext context) {
            boolean isMatch = rule.doMessage(message, context);
            return isMatch;
        }

        public List<Expression> getExpressions() {
            return expressions;
        }

        public int getRuleId() {
            return ruleId;
        }

        public Rule createRuleExpression() {
            RelationExpression relationExpression = new RelationExpression();
            relationExpression.setValue(new ArrayList<String>());
            relationExpression.setRelation("and");
            relationExpression.setConfigureName(NameCreatorContext.get().createNewName("_blink_rule_v2", "relation"));
            for (Expression expression : expressions) {
                String name = NameCreatorContext.get().createNewName("_blink_rule_v2", expression.getVarName());
                expression.setConfigureName(name);
                expression.setNameSpace("tmp");
                relationExpression.getValue().add(expression.getConfigureName());
            }
            Rule rule = ExpressionBuilder.createRule("tmp", "tmp", relationExpression, expressions);
            return rule;
        }

    }

    protected JSONObject createMsg(String[] kv) {
        JSONObject msg = null;
        if (kv.length % 2 == 0) {
            msg = new JSONObject();
            for (int i = 0; i < kv.length; i += 2) {
                msg.put(FunctionUtils.getConstant(kv[i]), kv[i + 1]);
            }
        }
        return msg;
    }

    public void parserExpressions() {
        InputStream inputStream = ReflectUtil.forClass(className).getResourceAsStream("/data_4_sas_black_rule_v2.json");
        List<String> rules = FileUtil.loadFileLine(inputStream);
        String line = rules.get(0);
        JSONArray allRules = JSON.parseArray(line);
        for (Object object : allRules) {
            JSONObject rule_json = JSON.parseObject(object.toString());
            int status = rule_json.getInteger("status");
            int ruleId = rule_json.getInteger("id");
            String modelId = rule_json.getString("model_id");
            String ruleValue = rule_json.getString("content");
            JSONObject ruleJson = JSONObject.parseObject(ruleValue);
            BlinkRule blinkRule = new BlinkRule(ruleJson, ruleId);
            if (status == 0) {
                if (!tmpWhiteListOnline.containsKey(modelId)) {
                    tmpWhiteListOnline.put(modelId, new ArrayList());
                }

                ((List) tmpWhiteListOnline.get(modelId)).add(blinkRule);
            }

            if (!tmpWhiteListOb.containsKey(modelId)) {
                tmpWhiteListOb.put(modelId, new ArrayList());
            }
            ((List) tmpWhiteListOb.get(modelId)).add(blinkRule);
        }
    }

    public static boolean isBlinkRuleV2Parser(IScriptExpression expression, IConfigurableService configurableService) {
        if (configurableService == null) {
            return false;
        }

        String functionName = expression.getFunctionName();
        if (functionName == null) {
            return false;
        }

        String name = MapKeyUtil.createKey("com.lyra.xs.udf.ext.sas_black_rule_v2", functionName).toLowerCase();

        UDFScript udfScript = configurableService.queryConfigurable(UDFScript.TYPE, name);

        if (udfScript != null && "com.lyra.xs.udf.ext.sas_black_rule_v2".equals(udfScript.getFullClassName())) {
            return true;
        }
        return false;
    }
}
