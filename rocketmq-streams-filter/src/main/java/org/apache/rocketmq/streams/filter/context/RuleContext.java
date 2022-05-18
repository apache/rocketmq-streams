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
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataAdapter;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.db.driver.JDBCDriver;
import org.apache.rocketmq.streams.filter.function.expression.ExpressionFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.function.service.impl.ScanFunctionService;

public class RuleContext extends AbstractContext<Message> implements Serializable {

    private static final Log LOG = LogFactory.getLog(RuleContext.class);

    /**
     * 观察者对应的configure
     */
    public static final String OBSERVER_NAME = "observerDBAction";

    private static RuleContext superRuleContext;

    private ExecutorService actionExecutor = null;

    /**
     * 默认的命名空间
     */
    public static final String DEFALUT_NAME_SPACE = IConfigurableService.PARENT_CHANNEL_NAME_SPACE;

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
    private transient RuleContext parentContext;
    private transient Rule rule;

    /**
     * 监控一条信息的运行情况
     */
    private transient volatile IMonitor ruleMonitor;

    /**
     * 一个规则对应的系统配置
     */
    private transient ContextConfigure contextConfigure;

    // private transient static volatile AtomicBoolean initflag = new AtomicBoolean(false);

    private static volatile boolean initflag = false;



    public static void initSuperRuleContext(ContextConfigure contextConfigure) {
        if (!initflag) {
            synchronized (RuleContext.class) {
                if (!initflag) {
                    RuleContext staticRuleContext = new RuleContext(DEFALUT_NAME_SPACE, contextConfigure);
                    ThreadFactory actionFactory = new ThreadFactoryBuilder().setNameFormat("RuleContext-Action-Poo-%d").build();
                    int threadSize = contextConfigure.getActionPoolSize();
                    ExecutorService actionExecutor = new ThreadPoolExecutor(threadSize, threadSize, 0L, TimeUnit.MILLISECONDS,
                        new LinkedBlockingQueue<>(1024), actionFactory, new ThreadPoolExecutor.AbortPolicy());
                    staticRuleContext.actionExecutor = actionExecutor;
                    staticRuleContext.functionService.scanePackage("org.apache.rocketmq.streams.filter.function");
                    superRuleContext = staticRuleContext;
                    initflag = true;
                }
            }
        }

    }

    private RuleContext(String pnameSpace, ContextConfigure contextConfigure) {
        this(pnameSpace, new JSONObject(), null, contextConfigure);
    }

    public RuleContext(String nameSpace, JSONObject pmessage, Rule rule, ContextConfigure contextConfigure) {
        super(new Message(pmessage));
        if (!DEFALUT_NAME_SPACE.equals(nameSpace)) {
            this.parentContext = superRuleContext;
        }
        this.nameSpace = nameSpace;
        this.rule = rule;
        this.contextConfigure = contextConfigure;
        if (contextConfigure == null && parentContext != null) {
            this.contextConfigure = this.parentContext.getContextConfigure();
        }
        if (this.functionService == null && this.parentContext != null) {
            this.functionService = this.parentContext.getFunctionService();
        }

    }

    public RuleContext(JSONObject pmessage, Rule rule, Properties properties) {
        this(rule.getNameSpace(), pmessage, rule, new ContextConfigure(properties));
    }

    public RuleContext(JSONObject pmessage, Rule rule) {
        this(rule.getNameSpace(), pmessage, rule, new ContextConfigure(null));
    }

    public String getNameSpace() {
        if (nameSpace != null) {
            return nameSpace;
        }
        if (parentContext != null) {
            return parentContext.getNameSpace();
        }
        return nameSpace;
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
            LOG.error("RuleContext getExpressionFunction error,name is: " + name, e);
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

    /**
     * 获取变量值,内部使用，不能直接使用，获取变量的值需要用var.getVarValue()
     *
     * @param varName
     * @return
     */
    public Object getVarCacheValue(String varName) {
        Object value = varValueMap.get(varName);
        if (value != null) {
            return value;
        }
        if (parentContext != null) {
            value = parentContext.getVarCacheValue(varName);
        }
        return value;

    }

    /**
     * 设置变量值
     *
     * @param varName
     * @param value
     */
    public void putVarValue(String nameSpace, String varName, Object value) {
        if (varName == null || value == null) {
            return;
        }
        if (rule.getVarMap().containsKey(varName)) {
            varValueMap.putIfAbsent(varName, value);
        }

    }

    public void setContextConfigure(ContextConfigure contextConfigure) {
        this.contextConfigure = contextConfigure;
    }

    public ContextConfigure getContextConfigure() {
        if (contextConfigure != null) {
            return contextConfigure;
        }
        if (parentContext != null) {
            return parentContext.getContextConfigure();
        }
        return null;
    }

    @Override
    public IConfigurableService getConfigurableService() {
        return rule.getConfigurableService();
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
        context.parentContext = parentContext;
        context.varValueMap = varValueMap;
        context.configurableService = configurableService;
        return context;
    }

    public ExecutorService getActionExecutor() {
        if (actionExecutor != null) {
            return actionExecutor;
        } else {
            return parentContext.getActionExecutor();
        }
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
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

    public RuleContext getParentContext() {
        return parentContext;
    }

    public void setParentContext(RuleContext parentContext) {
        this.parentContext = parentContext;
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

    public static void addNotFireExpressionMonitor(
        Object expression,AbstractContext context) {

        if(RelationExpression.class.isInstance(expression)){
            List<String> notFireExpressionMonitor=new ArrayList<>();
            RelationExpression relationExpression=(RelationExpression) expression;
            for(String expressionName:notFireExpressionMonitor){
                if(!relationExpression.getValue().contains(expressionName)){
                    notFireExpressionMonitor.add(expressionName);
                }
            }
            notFireExpressionMonitor.add(relationExpression.getConfigureName());
            context.setNotFireExpressionMonitor(notFireExpressionMonitor);
        }else if(Expression.class.isInstance(expression)) {
            Expression e=(Expression)expression;
            context.getNotFireExpressionMonitor().add(e.getConfigureName());
        }else if(String.class.isInstance(expression)){
            context.getNotFireExpressionMonitor().add((String)expression);
        }else {
            LOG.warn("can not support the express "+expression);
        }

    }
}
