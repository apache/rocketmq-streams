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
package org.apache.rocketmq.streams.filter.builder;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.ExpressionRelationParser;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.filter.operator.var.ContextVar;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.filter.optimization.ExpressionOptimization;

/**
 * 必须先初始化组件后才可以使用
 */
public class ExpressionBuilder {
    private static ListDataType listDataType = new ListDataType(DataTypeUtil.getDataTypeFromClass(String.class));
    private static ICache<String, Rule> cache = new SoftReferenceCache<>();

    /**
     * 根据表达式，创建一个规则，规则本身可执行
     *
     * @param namespace
     * @param expressionStr
     * @return
     */
    public static Rule createRule(String namespace, String ruleName, String expressionStr) {
        Rule rule = null;
        String key = MapKeyUtil.createKey(namespace, expressionStr);
        //rule = sinkcache.get(key);
        if (rule != null) {
            return rule;

        }
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = createExpression(namespace, ruleName, expressionStr, expressions, relationExpressions);
        ExpressionOptimization expressionOptimization = new ExpressionOptimization(expression, expressions, relationExpressions);
        List<Expression> expressionList = expressionOptimization.optimizate();
        rule = createRule(namespace, ruleName, expression, expressionList);
        cache.put(key, rule);
        return rule;
    }

    public static boolean executeExecute(String namespace, String expressionStr, IMessage message,
        AbstractContext context) {
        Rule rule = null;
        String key = expressionStr;
        rule = cache.get(key);
        if (rule == null) {
            rule = createRule(namespace, Rule.class.getSimpleName(), expressionStr);
            cache.put(key, rule);
        }
        return rule.doMessage(message, context);

    }

    /**
     * 可以简化表达式格式（varName,function,value)&((varName,function,datatype,value)|(varName,function,datatype,value))
     *
     * @param expressionStr
     * @param msg
     * @return
     */
    public static boolean executeExecute(String namespace, String expressionStr, JSONObject msg) {
        Rule rule = null;
        String key = expressionStr;
        rule = cache.get(key);
        if (rule == null) {
            rule = createRule(namespace, Rule.class.getSimpleName(), expressionStr);
            cache.put(key, rule);
        }
        return rule.execute(msg);

    }

    public static boolean executeExecute(Expression expression, IMessage message, AbstractContext context) {
        Rule rule = null;
        if (expression.getConfigureName() == null) {
            expression.setConfigureName(expression.getVarName());
        }
        String key = MapKeyUtil.createKey(expression.getNameSpace(), expression.toString());
        rule = cache.get(key);
        if (rule == null) {
            rule = createRule(expression.getNameSpace(), Rule.class.getSimpleName(), expression);
            cache.put(key, rule);
        }
        return rule.doMessage(message, context);
    }

    public static boolean executeExecute(Expression expression, JSONObject msg) {
        Rule rule = null;
        if (expression.getConfigureName() == null) {
            expression.setConfigureName(expression.getVarName());
        }
        String key = MapKeyUtil.createKey(expression.getNameSpace(), expression.toString());
        rule = cache.get(key);
        if (rule == null) {
            rule = createRule(expression.getNameSpace(), Rule.class.getSimpleName(), expression);
            cache.put(key, rule);
        }
        return rule.execute(msg);
    }

    /**
     * 创建一个表达式，并返回最顶层的表达式，中间的子表达式，全部relation表达式
     *
     * @param expressionStr
     * @return
     */
    public static Expression createExpression(String namespace, String ruleName, String expressionStr,
        List<Expression> expressions,
        List<RelationExpression> relationExpressions) {
        String relationStr = parseExpression(namespace, ruleName, expressionStr, expressions);
        if (expressions != null && expressions.size() == 1) {
            if (expressions.get(0).getConfigureName().equals(relationStr)) {
                Expression expression = expressions.get(0);
                ContextVar contextVar = new ContextVar();
                contextVar.setFieldName(expression.getVarName());
                expression.setVar(contextVar);
                return expressions.get(0);
            }
        }
        Expression relationExpression =
            ExpressionRelationParser.createRelations(namespace, ruleName, relationStr, relationExpressions);
        if (relationExpressions != null) {
            for (RelationExpression relation : relationExpressions) {
                relation.setDataType(listDataType);
            }
        }
        Map<String, Expression> map = new HashMap<>();
        for (Expression expression : expressions) {
            ContextVar contextVar = new ContextVar();
            contextVar.setFieldName(expression.getVarName());
            expression.setVar(contextVar);
            map.put(expression.getConfigureName(), expression);
        }
        for (RelationExpression relation : relationExpressions) {
            relation.setExpressionMap(map);
            map.put(relation.getConfigureName(), relation);
        }
        return relationExpression;
    }

    /**
     * 创建表达式，并做初步的优化，把同关系的多层relation，拉平。如((a,==,b)&(c,==,d))&(e,==,f)=>(a,==,b)&(c,==,d)&(e,==,f)
     *
     * @param expressionStr
     * @param expressions
     * @param relationExpressions
     * @return
     */
    public static Expression createOptimizationExpression(String namespace, String ruleName, String expressionStr,
        List<Expression> expressions,
        List<RelationExpression> relationExpressions) {
        Expression expression = createExpression(namespace, ruleName, expressionStr, expressions, relationExpressions);
        ExpressionOptimization expressionOptimization = new ExpressionOptimization(expression, expressions, relationExpressions);
        List<Expression> list = expressionOptimization.optimizate();
        if (list == null || list.size() == 0) {
            return expression;
        }
        expressions.clear();
        relationExpressions.clear();
        for (Expression express : list) {
            if (RelationExpression.class.isInstance(express)) {
                relationExpressions.add((RelationExpression) express);
            } else {
                expressions.add(express);
            }
        }
        return expression;
    }

    public static Rule createRule(String namespace, String ruleName, Expression expression,
        List<? extends Expression>... expressionLists) {
        RuleBuilder ruleCreator = new RuleBuilder(expression.getNameSpace(), ruleName);
        //RuleContext ruleContext=new RuleContext(msg,ruleCreator.createRule());
        Rule rule = ruleCreator.getRule();
        if (expressionLists != null) {
            for (List<? extends Expression> expressions : expressionLists) {
                if (expressions == null || expressions.size() == 0) {
                    continue;
                }
                for (Expression expression1 : expressions) {
                    createExpression(expression1, ruleCreator);
                }
                addConfigurable2Map(rule.getExpressionMap(), ruleCreator.getExpressionList());
            }
        }
        createExpression(expression, ruleCreator);

        addConfigurable2Map(rule.getVarMap(), ruleCreator.getVarList());
        addConfigurable2Map(rule.getExpressionMap(), ruleCreator.getExpressionList());
        addConfigurable2Map(rule.getMetaDataMap(), ruleCreator.getMetaDataList());
        addConfigurable2Map(rule.getDataSourceMap(), ruleCreator.getDataSourceList());
        addConfigurable2Map(rule.getActionMap(), ruleCreator.getActionList());
        ruleCreator.setRootExpression(expression);
        rule = ruleCreator.createRule();
        rule.initElements();
        ruleCreator.getMetaData().toObject(ruleCreator.getMetaData().toJson());//metadata的一个bug，如果不做这步，map为空。后续版本修复后可以去掉
        return rule;
    }

    /**
     * 一个表达式，需要的metafield，变量，value
     *
     * @param expression
     * @param ruleCreator
     */
    private static void createExpression(Expression expression, RuleBuilder ruleCreator) {
        if (RelationExpression.class.isInstance(expression)) {
            createRelationExpression(expression, ruleCreator);
        } else {
            createSingleExpression(expression, ruleCreator);
        }
    }

    /**
     * @param expression
     * @param ruleCreator
     */
    private static void createSingleExpression(Expression expression, RuleBuilder ruleCreator) {
        String varName = expression.getVarName();
        ruleCreator.addVarAndMetaDataField(varName);
        ruleCreator.addExpression(expression);
    }

    private static void createRelationExpression(Expression expression, RuleBuilder ruleCreator) {
        if (!RelationExpression.class.isInstance(expression)) {
            return;
        }
        ruleCreator.addExpression(expression);
    }

    private static <T extends IConfigurable> void addConfigurable2Map(Map<String, T> map, List<T> list) {
        if (map == null || list == null) {
            return;
        }
        for (T t : list) {
            if (Var.class.isInstance(t)) {
                map.put(((Var) t).getVarName(), t);
            } else {
                map.put(t.getConfigureName(), t);
            }

        }
    }

    /**
     * 把一个表达式组合，进行解析，返回一个关系字符串和一组表达式子列表expressions
     *
     * @param namespace
     * @param expressionStr
     * @param expressions
     * @return
     */
    private static String parseExpression(String namespace, String ruleName, String expressionStr,
        List<Expression> expressions) {
        return parseExpression(namespace, ruleName, expressionStr, expressions, ruleExpressionCreator);
    }

    public static void main(String[] args) {

    }

    /**
     * 把一个表达式组合，进行解析，返回一个关系字符串和一组表达式子列表expressions
     *
     * @param namespace
     * @param expressionStr
     * @param expressions
     * @return
     */
    public static String parseExpression(String namespace, String ruleName, String expressionStr,
        List<Expression> expressions,
        IRuleExpressionCreator creator) {
        if (StringUtil.isEmpty(expressionStr)) {
            return null;
        }

        expressionStr = expressionStr.replaceAll("\r\n", "");
        expressionStr = expressionStr.replaceAll("\n", "");

        expressionStr = ContantsUtil.replaceSpeciaSign(expressionStr);

        Map<String, String> flag2ExpressionStr = new HashMap<>();
        boolean containsContant = ContantsUtil.containContant(expressionStr);
        if (containsContant) {
            expressionStr = ContantsUtil.doConstantReplace(expressionStr, flag2ExpressionStr, 1);
        }
        String relationStr = expressionStr;
        NameCreator nameCreator = NameCreator.createOrGet(ruleName);
        if (expressionStr.indexOf("(") == -1) {
            relationStr = creator.createExpression(namespace, ruleName, expressionStr, expressions, containsContant, flag2ExpressionStr, nameCreator, relationStr);
            return relationStr;
        }

        Map<String, String> expressionFlags = new HashMap<>();
        relationStr = parseExpression(namespace, ruleName, expressionStr, expressions, expressionFlags, creator, containsContant, nameCreator, flag2ExpressionStr);
        boolean needReplace = true;
        while (needReplace) {
            String tmp = ContantsUtil.restore(relationStr, expressionFlags);
            if (tmp.equals(relationStr)) {
                relationStr = tmp;
                break;
            } else {
                relationStr = tmp;
            }
        }
        return relationStr;
        //        for (int i = 0; i < expressionStr.length(); i++) {
        //            String word = expressionStr.substring(i, i + 1);
        //
        //            if ("(".equals(word) ) {
        //                startExpression = true;
        //                continue;
        //            }
        //            if (")".equals(word)) {
        //                if (startExpression) {
        //                    String expresionStr = expressionSb.toString();
        //                    relationStr=creator.createExpression(namespace,ruleName,expresionStr,expressions,containsContant,flag2ExpressionStr,nameCreator,relationStr);
        //                    expressionSb = new StringBuilder();
        //                }
        //                startExpression = false;
        //                continue;
        //            }
        //            if (startExpression) {
        //                expressionSb.append(word);
        //            }
        //        }
        //        return relationStr;
    }

    /**
     * 把表达式中（a,b,c）部分解析出来，只留下关系部分
     *
     * @return 返回的是关系串
     */
    protected static String parseExpression(String namespace, String ruleName, String relationStr,
        List<Expression> expressions, Map<String, String> flag2ExpressionStr, IRuleExpressionCreator creator,
        boolean containsContant, NameCreator nameCreator, Map<String, String> contantsFlags) {
        int endIndex = relationStr.indexOf(")");
        if (endIndex == -1) {
            return relationStr;
        }
        if (relationStr.indexOf(",") == -1) {
            return relationStr;
        }
        for (int i = endIndex; i > 0; i--) {
            String word = relationStr.substring(i - 1, i);
            if (word.equals("(")) {
                String expressionStr = relationStr.substring(i, endIndex);
                String oriStr = "(" + expressionStr + ")";
                if (expressionStr.indexOf(",") != -1) {
                    relationStr = creator.createExpression(namespace, ruleName, expressionStr, expressions, containsContant, contantsFlags, nameCreator, relationStr);
                    relationStr = parseExpression(namespace, ruleName, relationStr, expressions, flag2ExpressionStr, creator, containsContant, nameCreator, contantsFlags);
                    return relationStr;
                } else {

                    String relationFlag = NameCreator.createNewName("relation");
                    flag2ExpressionStr.put(relationFlag, oriStr);
                    relationStr = relationStr.replace(oriStr, relationFlag);
                    return parseExpression(namespace, ruleName, relationStr, expressions, flag2ExpressionStr, creator, containsContant, nameCreator, contantsFlags);
                }
            }
        }
        return relationStr;
    }

    interface IRuleExpressionCreator {

        String createExpression(String namespace, String ruleName, String expresionStr, List<Expression> expressions,
            boolean containsContant,
            Map<String, String> flag2ExpressionStr, NameCreator nameCreator, String relationStr);
    }

    protected static IRuleExpressionCreator ruleExpressionCreator = new IRuleExpressionCreator() {
        @Override
        public String createExpression(String namespace, String ruleName, String expresionStr,
            List<Expression> expressions, boolean containsContant, Map<String, String> flag2ExpressionStr,
            NameCreator nameCreator, String relationStr) {
            String[] values = ExpressionBuilder.createElement(expresionStr, containsContant, flag2ExpressionStr);
            //expresionStr= ContantsUtil.restore(expresionStr,flag2ExpressionStr);
            String expressionName = nameCreator.createName(ruleName);

            SimpleExpression expression = null;
            if (values.length == 3) {
                String value = values[2];
                expression = new SimpleExpression(values[0], values[1], value);

            }
            if (values.length == 4) {
                String value = values[3];
                expression =
                    new SimpleExpression(values[0], values[1], DataTypeUtil.getDataType(values[2]), value);
            }
            if (expression != null) {
                expression.setNameSpace(namespace);
                expression.setConfigureName(expressionName);
                if (relationStr.indexOf("(") == -1) {
                    relationStr = relationStr.replace(expresionStr, expressionName);
                } else {
                    relationStr = relationStr.replace("(" + expresionStr + ")", expressionName);
                }

                expressions.add(expression);
            }
            return relationStr;
        }
    };

    public static String[] createElement(String expresionStr, boolean containsContant,
        Map<String, String> flag2ExpressionStr) {
        String[] values = expresionStr.split(",");
        int i = 0;
        for (String value : values) {
            if (containsContant) {
                value = ContantsUtil.restore(value, flag2ExpressionStr);
            }
            if (value != null && ContantsUtil.isContant(value)) {
                value = value.substring(1, value.length() - 1);
                values[i] = value;
            }
            values[i] = ContantsUtil.restoreSpecialSign(values[i]);
            i++;
        }
        return values;
    }
}
