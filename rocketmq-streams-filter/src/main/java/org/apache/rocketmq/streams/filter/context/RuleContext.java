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
package org.apache.rocketmq.streams.filter.context;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataAdapter;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.filter.function.expression.ExpressionFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RuleContext extends AbstractContext<Message> implements Serializable {

    /**
     * 观察者对应的configure
     */
    public static final String OBSERVER_NAME = "observerDBAction";
    private static final Logger LOGGER = LoggerFactory.getLogger(RuleContext.class);
    private static RuleContext superRuleContext;
    private static volatile boolean initflag = false;
    private ExecutorService actionExecutor = null;
    /**
     * var的名称合值对应的map
     */
    private ConcurrentMap<String, Object> varValueMap = new ConcurrentHashMap<String, Object>();
    private ConcurrentMap<String, Boolean> expressionValueMap = new ConcurrentHashMap<String, Boolean>();
    /**
     * 错误信息
     */
    private Vector<String> errorMessageList = new Vector<>();
    /**
     * 当前的命名空间
     */
    private String nameSpace;
    /**
     * 配置服务
     */
    //    private transient IConfigurableService configureService;

    private transient ScanFunctionService functionService = ScanFunctionService.getInstance();
    private transient Rule rule;
    /**
     * 监控一条信息的运行情况
     */
    private transient volatile IMonitor ruleMonitor;

    // private transient static volatile AtomicBoolean initflag = new AtomicBoolean(false);
    /**
     * 一个规则对应的系统配置
     */
    private transient ContextConfigure contextConfigure;

    private RuleContext(String pnameSpace, ContextConfigure contextConfigure) {
        this(pnameSpace, new JSONObject(), null, contextConfigure);
    }

    public RuleContext(String nameSpace, JSONObject pmessage, Rule rule, ContextConfigure contextConfigure) {
        super(new Message(pmessage));
        this.nameSpace = nameSpace;
        this.rule = rule;
        this.contextConfigure = contextConfigure;

    }

    public RuleContext(JSONObject pmessage, Rule rule, Properties properties) {
        this(rule.getNameSpace(), pmessage, rule, new ContextConfigure(properties));
    }

    public RuleContext(JSONObject pmessage, Rule rule) {
        this(rule.getNameSpace(), pmessage, rule, new ContextConfigure(null));
    }

    public static void addNotFireExpressionMonitor(
        Object expression, AbstractContext context) {

        if (RelationExpression.class.isInstance(expression)) {
            List<String> notFireExpressionMonitor = new ArrayList<>();
            RelationExpression relationExpression = (RelationExpression) expression;
            notFireExpressionMonitor.add(relationExpression.getName());
            context.setNotFireExpressionMonitor(notFireExpressionMonitor);
        } else if (Expression.class.isInstance(expression)) {
            Expression e = (Expression) expression;
            context.getNotFireExpressionMonitor().add(e.getName());
        } else if (String.class.isInstance(expression)) {
            context.getNotFireExpressionMonitor().add((String) expression);
        } else {
            LOGGER.warn("can not support the express " + expression);
        }

    }

    public String getNameSpace() {
        if (nameSpace != null) {
            return nameSpace;
        }
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    /**
     * 获取表达式值
     *
     * @param configureName
     * @return
     */
    public Boolean getExpressionValue(String configureName) {
        Boolean result = expressionValueMap.get(configureName);
        return result;
    }

    /**
     * 设置表达式值
     *
     * @param configureName
     * @param result
     */
    public void putExpressionValue(String nameSpace, String configureName, Boolean result) {
        if (rule.getExpressionMap().containsKey(configureName)) {
            expressionValueMap.putIfAbsent(configureName, result);
        }
    }

    /**
     * 当规则产生错误时，记录错误信息
     *
     * @param message
     */
    public void addErrorMessage(Rule rule, String message) {
        String messageInfo = message;
        if (rule != null) {
            messageInfo = rule.getRuleCode() + ":" + messageInfo;
        }
        errorMessageList.add(messageInfo);
    }

    public Var getVar(String ruleName, String name) {
        return getVar(name);
    }

    public Var getVar(String name) {
        Var var = rule.getVarMap().get(name);

        return var;
    }

    public Expression getExpression(String name) {
        Expression expression = rule.getExpressionMap().get(name);
        return expression;
    }

    public Action getAction(String name) {
        Action action = rule.getActionMap().get(name);
        return action;
    }

    public ExpressionFunction getExpressionFunction(String name, Object... objects) {
        try {
            FunctionConfigure fc = functionService.getFunctionConfigure(name, objects);
            if (fc == null) {
                return null;
            }
            return (ExpressionFunction) fc.getBean();
        } catch (Exception e) {
            LOGGER.error("RuleContext getExpressionFunction error,name is: " + name, e);
            return null;
        }

    }

    public MetaData getMetaData(String name) {
        MetaData metaData = rule.getMetaDataMap().get(name);
        return metaData;
    }

    public JDBCDriver getDataSource(String name) {
        return rule.getDataSourceMap().get(name);
    }

    /**
     * @param name
     * @return
     */
    public MetaDataAdapter getMetaDataAdapter(String name) {
        MetaData md = getMetaData(name);
        JDBCDriver dataSource = this.getDataSource(md.getDataSourceName());
        MetaDataAdapter mda = new MetaDataAdapter(md, dataSource);
        return mda;

    }

    public boolean containsVarName(String varName) {
        if (varValueMap.containsKey(varName)) {
            return true;
        }
        return false;
    }

    @Override
    public AbstractContext copy() {
        IMessage message = this.message.deepCopy();
        RuleContext context = new RuleContext(nameSpace, message.getMessageBody(), rule, contextConfigure);
        super.copyProperty(context);
        context.actionExecutor = actionExecutor;
        context.errorMessageList = errorMessageList;
        context.expressionValueMap = expressionValueMap;
        context.functionService = functionService;
        context.ruleMonitor = ruleMonitor;
        context.varValueMap = varValueMap;
        return context;
    }

    public ConcurrentMap<String, Object> getVarValueMap() {
        return varValueMap;
    }

    public void setVarValueMap(ConcurrentMap<String, Object> varValueMap) {
        this.varValueMap = varValueMap;
    }

    public ConcurrentMap<String, Boolean> getExpressionValueMap() {
        return expressionValueMap;
    }

    public void setExpressionValueMap(ConcurrentMap<String, Boolean> expressionValueMap) {
        this.expressionValueMap = expressionValueMap;
    }

    public Vector<String> getErrorMessageList() {
        return errorMessageList;
    }

    public void setErrorMessageList(Vector<String> errorMessageList) {
        this.errorMessageList = errorMessageList;
    }

    public ScanFunctionService getFunctionService() {
        return functionService;
    }

    public Rule getRule() {
        return rule;
    }

    public void setRule(Rule rule) {
        this.rule = rule;
    }

    public IMonitor getRuleMonitor() {
        return ruleMonitor;
    }

    public void setRuleMonitor(IMonitor ruleMonitor) {
        this.ruleMonitor = ruleMonitor;
    }
}
