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
package org.apache.rocketmq.streams.script.operator.expression;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.serviceloader.ServiceLoaderComponent;

/**
 * 条件函数，支持if elseif else 每一个部分又是用IScriptExpression来表达。 执行流程，如果if成功执行then，循环执行elseif，最后执行else
 */
public class GroupScriptExpression implements IScriptExpression {

    private static final String TAB = FunctionParser.TAB;
    protected List<? extends AbstractRule> rules;
    protected List<GroupScriptExpression> elseIfExpressions = new ArrayList<>();
    protected List<IScriptExpression> beforeExpressions;
    protected List<IScriptExpression> afterExpressions;
    private IScriptExpression ifExpresssion;
    private transient String boolVar;
    private transient AbstractRule rule;
    private List<IScriptExpression> thenExpresssions;
    private List<IScriptExpression> elseExpressions;
    private String scriptParameterStr;
    private transient IGroupScriptOptimization caseDependentParser;

    @Override
    public Object getScriptParamter(IMessage message, FunctionContext context) {
        return this.executeExpression(message, context);
    }

    @Override
    public String getScriptParameterStr() {
        return scriptParameterStr;
    }

    public void setScriptParameterStr(String scriptParameterStr) {
        this.scriptParameterStr = scriptParameterStr;
    }

    @Override
    public Object executeExpression(IMessage channelMessage, FunctionContext context) {
        executeBeforeExpression(channelMessage, context);
        Boolean result = executeIf(this, channelMessage, context);

        Object value = null;
        if (result) {
            if (thenExpresssions != null) {
                for (IScriptExpression scriptExpression : thenExpresssions) {
                    scriptExpression.executeExpression(channelMessage, context);
                }
            }
            return executeAfterExpression(channelMessage, context);
        }
        if (elseIfExpressions != null && elseIfExpressions.size() > 0) {
            for (int i = elseIfExpressions.size() - 1; i >= 0; i--) {
                GroupScriptExpression expression = elseIfExpressions.get(i);
                boolean success = executeIf(expression, channelMessage, context);
                if (success) {
                    if (expression.thenExpresssions != null) {
                        for (IScriptExpression scriptExpression : expression.thenExpresssions) {
                            value = scriptExpression.executeExpression(channelMessage, context);
                        }
                    }
                    return executeAfterExpression(channelMessage, context);
                }
            }
        }

        if (elseExpressions != null) {
            for (IScriptExpression scriptExpression : elseExpressions) {
                scriptExpression.executeExpression(channelMessage, context);
                return executeAfterExpression(channelMessage, context);
            }
        }
        return null;
    }

    protected Boolean executeIf(GroupScriptExpression groupScriptExpression, IMessage message, FunctionContext context) {
        if (StringUtil.isNotEmpty(groupScriptExpression.boolVar)) {
            return message.getMessageBody().getBooleanValue(groupScriptExpression.boolVar);
        } else if (groupScriptExpression.rule != null) {
            return groupScriptExpression.rule.doMessage(message, context);
        } else {
            Boolean result = (Boolean) groupScriptExpression.ifExpresssion.executeExpression(message, context);
            return result;
        }
    }

    public void executeBeforeExpression(IMessage channelMessage, FunctionContext context) {
        if (beforeExpressions != null) {
            for (IScriptExpression scriptExpression : beforeExpressions) {
                scriptExpression.executeExpression(channelMessage, context);
            }
        }

    }

    protected Object executeAfterExpression(IMessage channelMessage, FunctionContext context) {
        Object object = null;
        if (afterExpressions != null) {
            for (IScriptExpression scriptExpression : afterExpressions) {
                object = scriptExpression.executeExpression(channelMessage, context);
            }
        }
        return object;
    }

    @Override
    public List<IScriptParamter> getScriptParamters() {
        return null;
    }

    @Override
    public String getFunctionName() {
        return "condition";
    }

    @Override
    public String getExpressionDescription() {
        return toString();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (ifExpresssion != null) {
            sb.append(ifExpresssion.toString());
        }

        if (thenExpresssions != null) {
            sb.append("{" + PrintUtil.LINE);
            boolean isFirst = true;
            for (IScriptExpression scriptExpression : thenExpresssions) {
                sb.append(TAB + scriptExpression + ";" + PrintUtil.LINE);
            }
            sb.append("}");
        }
        if (elseExpressions != null) {
            sb.append("else{" + PrintUtil.LINE);
            for (IScriptExpression expression : this.elseExpressions) {
                sb.append(TAB + expression + ";" + PrintUtil.LINE);
            }
            sb.append("}");
        }

        return sb.toString();
    }

    @Override
    public List<String> getDependentFields() {
        List<GroupScriptExpression> list = new ArrayList<>();
        list.add(this);
        if (elseIfExpressions != null) {
            list.addAll(elseIfExpressions);
        }
        List<String> dependentFields = new ArrayList<>();
        for (GroupScriptExpression groupScriptExpression : list) {
            List<String> fields = groupScriptExpression.getDependentFieldsInner();
            if (fields != null) {
                dependentFields.addAll(fields);
            }
        }
        return dependentFields;

    }

    @Override
    public Set<String> getNewFieldNames() {
        List<GroupScriptExpression> list = new ArrayList<>();
        list.add(this);
        if (elseIfExpressions != null) {
            list.addAll(elseIfExpressions);
        }
        Set<String> newFields = new HashSet<>();
        for (GroupScriptExpression groupScriptExpression : list) {
            Set<String> fields = groupScriptExpression.getNewFieldNamesInner();
            if (fields != null) {
                newFields.addAll(fields);
            }
        }
        return newFields;
    }

    protected List<String> getDependentFieldsInner() {
        List<String> fieldNames = new ArrayList<>();
        List<IScriptExpression> parameters = new ArrayList<>();
        if (ifExpresssion != null) {
            parameters.add(ifExpresssion);
        }
        if (thenExpresssions != null) {
            parameters.addAll(thenExpresssions);
        }
        if (elseIfExpressions != null) {
            parameters.addAll(elseIfExpressions);
        }
        IGroupScriptOptimization caseDependentParser = loadCaseDependentParser();
        if (parameters != null && parameters.size() > 0) {
            for (IScriptExpression scriptExpression : parameters) {
                List<String> names = null;
                if (caseDependentParser != null && caseDependentParser.isCaseFunction(scriptExpression)) {
                    names = new ArrayList<>(caseDependentParser.getDependentFields(scriptExpression));
                } else {
                    names = scriptExpression.getDependentFields();
                }
                if (names != null) {
                    fieldNames.addAll(names);
                }
            }
        }
        return fieldNames;
    }

    public Set<String> getIFDependentFields() {
        Set<String> fieldNames = new HashSet<>();
        List<IScriptExpression> parameters = new ArrayList<>();
        if (ifExpresssion != null) {
            parameters.add(ifExpresssion);
        }

        IGroupScriptOptimization caseDependentParser = loadCaseDependentParser();
        if (ifExpresssion != null && parameters.size() > 0) {
            for (IScriptExpression scriptExpression : parameters) {
                List<String> names = null;
                if (caseDependentParser != null && caseDependentParser.isCaseFunction(scriptExpression)) {
                    names = new ArrayList<>(caseDependentParser.getDependentFields(scriptExpression));
                } else {
                    names = scriptExpression.getDependentFields();
                }
                if (names != null) {
                    fieldNames.addAll(names);
                }
            }
        }
        return fieldNames;

    }

    protected Set<String> getNewFieldNamesInner() {
        Set<String> set = new HashSet<>();
        List<IScriptExpression> parameters = new ArrayList<>();
        if (thenExpresssions != null) {
            parameters.addAll(thenExpresssions);
        }
        if (elseIfExpressions != null) {
            parameters.addAll(elseIfExpressions);
        }
        if (parameters != null && parameters.size() > 0) {
            for (IScriptParamter scriptParamter : parameters) {
                Set<String> names = scriptParamter.getNewFieldNames();
                if (names != null) {
                    set.addAll(names);
                }
            }
        }
        return set;
    }

    protected IGroupScriptOptimization loadCaseDependentParser() {
        if (this.caseDependentParser != null) {
            return this.caseDependentParser;
        }
        synchronized (this) {
            if (caseDependentParser != null) {
                return this.caseDependentParser;
            }
            ServiceLoaderComponent caseServiceLoader = ServiceLoaderComponent.getInstance(IGroupScriptOptimization.class);

            List<IGroupScriptOptimization> caseDependentParsers = caseServiceLoader.loadService();
            IGroupScriptOptimization caseDependentParser = null;
            if (caseDependentParsers != null && caseDependentParsers.size() > 0) {
                caseDependentParser = caseDependentParsers.get(0);
                caseDependentParser.compile(this);
            }
            return caseDependentParser;
        }

    }

    public List<GroupScriptExpression> getElseIfExpressions() {
        return elseIfExpressions;
    }

    public void setElseIfExpressions(List<GroupScriptExpression> elseIfExpressions) {
        this.elseIfExpressions = elseIfExpressions;
    }

    public IScriptExpression getIfExpresssion() {
        return ifExpresssion;
    }

    public void setIfExpresssion(IScriptExpression ifExpresssion) {
        this.ifExpresssion = ifExpresssion;
    }

    public List<IScriptExpression> getBeforeExpressions() {
        return beforeExpressions;
    }

    public void setBeforeExpressions(
        List<IScriptExpression> beforeExpressions) {
        this.beforeExpressions = beforeExpressions;
    }

    public List<IScriptExpression> getAfterExpressions() {
        return afterExpressions;
    }

    public void setAfterExpressions(
        List<IScriptExpression> afterExpressions) {
        this.afterExpressions = afterExpressions;
    }

    public List<IScriptExpression> getElseExpressions() {
        return elseExpressions;
    }

    public void setElseExpressions(List<IScriptExpression> elseExpressions) {
        this.elseExpressions = elseExpressions;
    }

    public List<? extends AbstractRule> getRules() {
        return rules;
    }

    public void setRules(List<? extends AbstractRule> rules) {
        this.rules = rules;
    }

    public List<IScriptExpression> getThenExpresssions() {
        return thenExpresssions;
    }

    public void setThenExpresssions(List<IScriptExpression> thenExpresssions) {
        this.thenExpresssions = thenExpresssions;
    }

    public Map<? extends String, ? extends List<String>> getBeforeDependents() {
        Map<String, List<String>> map = new HashMap<>();
        if (CollectionUtil.isEmpty(beforeExpressions)) {
            return map;
        }

        for (IScriptExpression scriptExpression : beforeExpressions) {
            if (CollectionUtil.isNotEmpty(scriptExpression.getNewFieldNames())) {
                map.put(scriptExpression.getNewFieldNames().iterator().next(), scriptExpression.getDependentFields());
            }
        }
        return map;
    }

    public String getBoolVar() {
        return boolVar;
    }

    public void setBoolVar(String boolVar) {
        this.boolVar = boolVar;
    }

    public AbstractRule getRule() {
        return rule;
    }

    public void setRule(AbstractRule rule) {
        this.rule = rule;
    }
}
