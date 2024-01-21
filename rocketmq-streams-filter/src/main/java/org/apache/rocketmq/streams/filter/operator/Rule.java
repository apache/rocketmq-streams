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
package org.apache.rocketmq.streams.filter.operator;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.filter.FilterComponent;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.filter.operator.action.impl.SinkAction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.GroupExpression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.var.ContextVar;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.filter.optimization.ExpressionOptimization;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Rule extends AbstractRule implements
    IStageBuilder<AbstractChainStage<?>> {
    public static final String FIRE_RULES = "fireRules";
    private static final Logger LOGGER = LoggerFactory.getLogger(Rule.class);
    private transient static FilterComponent filterComponent = FilterComponent.getInstance();
    protected transient Expression rootExpression;
    private transient volatile Map<String, Var> varMap = new HashMap<>();
    private transient volatile Map<String, Expression> expressionMap = new HashMap<>();
    @Deprecated
    private transient Map<String, Action> actionMap = new HashMap<>();
    private transient Map<String, MetaData> metaDataMap = new HashMap<>();
    private transient volatile Map<String, JDBCDriver> dataSourceMap = new HashMap<>();
    private String expressionStr;//表达式
    /**
     * 如果已经完成varmap和expressionmap的初始化,主要是用于兼容老版本规则数据，新规则可以忽略这个字段，值设置为true
     */
    private transient boolean isFinishVarAndExpression = false;

    public Rule() {
    }

    public Rule(String namespace, String name, String expression) {
        this();
        FilterComponent filterComponent = FilterComponent.getInstance();
        Rule rule = filterComponent.createRule(namespace, name, expression);
        this.varMap = rule.getVarMap();
        this.actionMap = rule.getActionMap();
        this.expressionMap = rule.getExpressionMap();
        this.metaDataMap = rule.getMetaDataMap();
        this.dataSourceMap = rule.getDataSourceMap();
        this.setMsgMetaDataName(rule.getMsgMetaDataName());
        this.setExpressionStr(rule.getExpressionStr());
        this.setVarNames(rule.getVarNames());
        this.setExpressionName(rule.getExpressionName());
        this.rootExpression = this.expressionMap.get(getExpressionName());
        initElements();
    }

    public Rule(String expression) {
        this(null, null, expression);
    }

    public Rule copy() {
        Rule rule = new Rule();
        rule.setNameSpace(getNameSpace());
        rule.setType(getType());
        rule.setName(getName());
        rule.varMap = varMap;
        rule.expressionMap = expressionMap;
        rule.actionMap = actionMap;
        rule.metaDataMap = metaDataMap;
        rule.dataSourceMap = dataSourceMap;
        rule.setActionNames(actionNames);
        rule.setVarNames(varNames);
        rule.setExpressionName(expressionName);
        rule.setMsgMetaDataName(getMsgMetaDataName());
        rule.setRuleCode(ruleCode);
        rule.setRuleDesc(ruleDesc);
        rule.setRuleStatus(ruleStatus);
        rule.setRuleTitle(ruleTitle);
        rule.setRuleStatus(ruleStatus);
        rule.rootExpression = rootExpression;
        return rule;
    }

    public void initElements() {

        for (Expression expression : this.expressionMap.values()) {
            if (RelationExpression.class.isInstance(expression)) {
                RelationExpression relationExpression = (RelationExpression) expression;
                relationExpression.setExpressionMap(this.expressionMap);
            } else {
                Var var = createVar(expression.getVarName());
                expression.setVar(var);
            }
        }
        for (Action action : this.actionMap.values()) {
            action.setDataSourceMap(this.dataSourceMap);
            action.setMetaDataMap(this.metaDataMap);
        }
    }

    public void addAction(SinkAction action) {
        actionMap.put(action.getName(), action);
        this.getActionNames().add(action.getName());
    }

    @Override
    public Set<String> getDependentFields() {
        Expression expression = expressionMap.get(getExpressionName());
        return expression.getDependentFields(expressionMap);
    }

    /**
     * 递归获取所有依赖的expression
     *
     * @param expressionName
     * @param expressionMap
     */
    protected void fetchExpression(String expressionName, Map<String, Expression> expressionMap) {
        Expression expression = expressionMap.get(expressionName);
        if (expression == null) {
            return;
        }
        this.expressionMap.put(expression.getName(), expression);
        if (!RelationExpression.class.isInstance(expression)) {
            return;
        }
        RelationExpression relationExpression = (RelationExpression) expression;
        List<String> expressionNames = relationExpression.getValue();
        if (expressionNames != null) {
            for (String name : expressionNames) {
                fetchExpression(name, expressionMap);
            }
        }
    }

    protected Var createVar(String varName) {
        ContextVar contextVar = new ContextVar();
        contextVar.setNameSpace(getNameSpace());
        contextVar.setName(varName);
        contextVar.setVarName(varName);
        contextVar.setFieldName(varName);
        return contextVar;
    }

    public void putConfigurableMap(IConfigurable configurable, String type) {
        if (Var.TYPE.equals(type)) {
            varMap.put(configurable.getName(), (Var) configurable);
        } else if (Expression.TYPE.equals(type)) {
            expressionMap.put(configurable.getName(), (Expression) configurable);
        } else if (Action.TYPE.equals(type)) {
            actionMap.put(configurable.getName(), (Action) configurable);
        } else if (MetaData.TYPE.equals(type)) {
            metaDataMap.put(configurable.getName(), (MetaData) configurable);
        } else if (ISink.TYPE.equals(type)) {
            dataSourceMap.put(configurable.getName(), (JDBCDriver) configurable);
        }
    }

    public List<IConfigurable> getDependConfigurables() {
        List<IConfigurable> configurableList = new ArrayList<>();
        if (varMap != null) {
            configurableList.addAll(varMap.values());
        }
        if (expressionMap != null) {
            configurableList.addAll(expressionMap.values());
        }
        if (actionMap != null) {
            configurableList.addAll(actionMap.values());
        }
        if (metaDataMap != null) {
            configurableList.addAll(metaDataMap.values());
        }
        if (dataSourceMap != null) {
            configurableList.addAll(dataSourceMap.values());
        }
        return configurableList;
    }

    public Map<String, Var> getVarMap() {
        return varMap;
    }

    public void setVarMap(Map<String, Var> varMap) {
        this.varMap = varMap;
    }

    public Map<String, Expression> getExpressionMap() {
        return expressionMap;
    }

    public void setExpressionMap(Map<String, Expression> expressionMap) {
        this.expressionMap = expressionMap;
    }

    public Map<String, Action> getActionMap() {
        return actionMap;
    }

    public void setActionMap(Map<String, Action> actionMap) {
        this.actionMap = actionMap;
    }

    public Map<String, MetaData> getMetaDataMap() {
        return metaDataMap;
    }

    public void setMetaDataMap(Map<String, MetaData> metaDataMap) {
        this.metaDataMap = metaDataMap;
    }

    public Map<String, JDBCDriver> getDataSourceMap() {
        return dataSourceMap;
    }

    public void setDataSourceMap(Map<String, JDBCDriver> dataSourceMap) {
        this.dataSourceMap = dataSourceMap;
    }

    @Override
    public Boolean doMessage(IMessage message, AbstractContext context) {
        boolean isTrace = TraceUtil.hit(message.getHeader().getTraceId());
        context.setNotFireExpressionMonitor(new ArrayList<>());
        boolean isFireRule = processExpress(message, context, isTrace);
        return isFireRule;

    }

    public boolean execute(JSONObject msg) {
        Message message = new Message(msg);
        AbstractContext context = new Context(message);
        return doMessage(message, context);
    }

    private JSONArray createJsonArray(List<Rule> fireRules) {
        JSONArray jsonArray = new JSONArray();
        if (fireRules == null) {
            return jsonArray;
        }
        for (Rule rule : fireRules) {
            jsonArray.add(rule.getNameSpace() + ":" + rule.getName());
        }
        return jsonArray;
    }

    public String toExpressionString() {
        Expression rootExpression = expressionMap.get(expressionName);
        return rootExpression.toExpressionString(expressionMap);
    }

    public String toMetaDataString() {
        MetaData metaData = metaDataMap.get(getMsgMetaDataName());
        if (metaData == null) {
            return null;
        }
        List<MetaDataField> metaDataFields = metaData.getMetaDataFields();
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;
        for (MetaDataField metaDataField : metaDataFields) {
            if (metaDataField.getDataType().matchClass(String.class)) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                sb.append(",");
            }
            String line = metaDataField.getFieldName() + ";" + metaDataField.getDataType().getDataTypeName();
            sb.append(line);
        }
        return sb.toString();
    }

    @Override public String toString() {
        return createOptimizationRule().toExpressionString(this.expressionMap);
    }

    public boolean isFinishVarAndExpression() {
        return isFinishVarAndExpression;
    }

    public void setFinishVarAndExpression(boolean finishVarAndExpression) {
        isFinishVarAndExpression = finishVarAndExpression;
    }

    public String getExpressionStr() {
        return expressionStr;
    }

    public void setExpressionStr(String expressionStr) {
        this.expressionStr = expressionStr;
    }

    @Override
    public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        FilterChainStage filterChainStage = new FilterChainStage();
        pipelineBuilder.addConfigurables(this);
        filterChainStage.setRule(this);
        filterChainStage.setEntityName("filter");
        filterChainStage.setLabel(this.getName());
        return filterChainStage;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        if (expressionMap.values() != null) {
            pipelineBuilder.addConfigurables(expressionMap.values());
        }
        if (actionMap.values() != null) {
            pipelineBuilder.addConfigurables(actionMap.values());
        }
        if (metaDataMap.values() != null) {
            pipelineBuilder.addConfigurables(metaDataMap.values());
        }
        if (dataSourceMap.values() != null) {
            pipelineBuilder.addConfigurables(dataSourceMap.values());
        }
    }

    /**
     * 做优化，把相同变量的表达式，用hyperscan执行
     */
    public void optimize() {
        Expression root = createOptimizationRule();
//        if (!RelationExpression.class.isInstance(root)) {
//            return;
//        }
//        groupByChildrenExpression((RelationExpression) root);
    }

    /**
     * 在同一层关系中，相同变量名的分到一组，统一做处理
     *
     * @param root
     */
    protected void groupByChildrenExpression(RelationExpression root) {
        List<String> expressionNames = root.getValue();
        if (expressionNames == null || expressionNames.size() == 0) {
            return;
        }
        Map<String, GroupExpression> varName2ExpressionNames = new HashMap<>();
        List<String> newExpressionNames = new ArrayList<>();
        for (String name : expressionNames) {
            Expression expression = expressionMap.get(name);
            //是直接值，不需要再次计算
            if (expression == null) {
                newExpressionNames.add(name);
                continue;
            }

            //递归分组
            if (RelationExpression.class.isInstance(expression)) {
                newExpressionNames.add(name);
                RelationExpression relationExpression = (RelationExpression) expression;
                groupByChildrenExpression(relationExpression);
                continue;
            }
            //按变量名分组
            String varName = expression.getVarName();
            GroupExpression groupExpression = varName2ExpressionNames.get(varName);
            if (groupExpression == null) {
                groupExpression = new GroupExpression(this, this.varMap.get(varName), root.getRelation().equalsIgnoreCase("or"));
                varName2ExpressionNames.put(varName, groupExpression);

            }
            groupExpression.addExpressionName(expression);
        }

        //如果某个变量的个数>5，去除掉原来多个表达式，换成分组表达式，否则保持不变
        Iterator<Entry<String, GroupExpression>> it = varName2ExpressionNames.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, GroupExpression> entry = it.next();
            GroupExpression groupExpression = entry.getValue();
            if (groupExpression.size() < 2) {
                newExpressionNames.addAll(groupExpression.getAllExpressionNames());
            } else {
                expressionMap.put(groupExpression.getName(), groupExpression);
                newExpressionNames.add(groupExpression.getName());
            }
        }
        root.setValue(newExpressionNames);
    }

    /**
     * 默认规则的解析是多层嵌套，优化的方案是尽量展开
     */
    protected Expression createOptimizationRule() {
        Expression root = expressionMap.get(expressionName);
        if (!RelationExpression.class.isInstance(root)) {
            return root;
        }
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Iterator<Expression> it = expressionMap.values().iterator();
        while (it.hasNext()) {
            Expression expression = it.next();
            if (RelationExpression.class.isInstance(expression)) {
                relationExpressions.add((RelationExpression) expression);
            } else {
                expressions.add(expression);
            }
        }
        ExpressionOptimization expressionOptimization = new ExpressionOptimization(root, expressions, relationExpressions);
        List<Expression> list = expressionOptimization.optimizate();
        expressionMap.clear();
        expressions.clear();
        relationExpressions.clear();
        for (Expression express : list) {
            expressionMap.put(express.getName(), express);
            if (RelationExpression.class.isInstance(express)) {
                relationExpressions.add((RelationExpression) express);
            } else {
                expressions.add(express);
            }
        }
        return root;
    }

    /**
     * 处理Express
     *
     * @param context
     * @return
     */
    @SuppressWarnings("rawtypes")
    private boolean processExpress(IMessage message, AbstractContext context, boolean isTrace) {
        try {
            if (getExpressionName() == null) {
                return false;
            }

            Expression expression = this.rootExpression;
            if (expression == null) {
                throw new RuntimeException("need root expression");
            }

            boolean match = expression.doMessage(message, context);
            if (!RelationExpression.class.isInstance(expression)) {
                RuleContext.addNotFireExpressionMonitor(expression, context);
            }
            if (!match) {
                return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("DefaultRuleEngine processExpress error,rule is: " + getName(), e);
            return false;
        }
        return true;
    }

    public Expression getRootExpression() {
        return rootExpression;
    }

    public void setRootExpression(Expression rootExpression) {
        this.rootExpression = rootExpression;
    }
}
