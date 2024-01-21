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
package org.apache.rocketmq.streams.filter.optimization.casewhen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.RuleExpression;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class CaseWhenElement {
    protected Rule rule;
    protected List<IScriptExpression> thenScriptExpressionList;
    protected List<IScriptExpression> beforeExpressions;

    protected transient Map<String, IScriptExpression> varName2Scripts = new HashMap<>();
    protected transient Map<String, List<String>> varName2DependentFields = new HashMap<>();

    public CaseWhenElement(GroupScriptExpression groupScriptExpression) {
        this(groupScriptExpression, false);
    }

    public CaseWhenElement(GroupScriptExpression groupScriptExpression, boolean isCompressBeforeExpression) {
        if (groupScriptExpression.getElseIfExpressions() != null && groupScriptExpression.getElseIfExpressions().size() > 0) {
            throw new RuntimeException("can not support has elseif groupScriptExpression, please use CaseWhenFactory");
        }
        this.thenScriptExpressionList = groupScriptExpression.getThenExpresssions();
        if (groupScriptExpression.getAfterExpressions() != null) {
            this.thenScriptExpressionList.addAll(groupScriptExpression.getAfterExpressions());
        }

        IScriptExpression ifExpression = groupScriptExpression.getIfExpresssion();
        if (RuleExpression.class.isInstance(ifExpression)) {
            this.rule = ((RuleExpression) ifExpression).getRule();
        } else {
            if (!CaseFunction.isCaseFunction(ifExpression.getFunctionName())) {
                throw new RuntimeException("can not support not casefunction's ifExpression");
            }
            String ifStr = IScriptOptimization.getParameterValue((IScriptParamter) ifExpression.getScriptParamters().get(0));
            this.rule = ExpressionBuilder.createRule("tmp", "tmp", ifStr);
        }

        if (isCompressBeforeExpression) {
            this.beforeExpressions = addBeforeExpressions(groupScriptExpression.getBeforeExpressions());
        } else {
            this.beforeExpressions = groupScriptExpression.getBeforeExpressions();
        }
    }

    public boolean executeCase(IMessage message, AbstractContext context) {
        if (beforeExpressions != null) {
            for (IScriptExpression scriptExpression : beforeExpressions) {
                scriptExpression.executeExpression(message, (FunctionContext) context);
            }
        }
        return rule.doMessage(message, context);
    }

    public boolean executeDirectly(IMessage message, AbstractContext context) {
        boolean isFire = executeCase(message, context);
        if (isFire) {
            executeThen(message, context);
        }
        return isFire;
    }

    public Object executeThen(IMessage message, AbstractContext context) {
        if (thenScriptExpressionList != null) {
            for (IScriptExpression scriptExpression : thenScriptExpressionList) {
                scriptExpression.executeExpression(message, (FunctionContext) context);
            }
        }
        return null;
    }

    protected List<IScriptExpression> addBeforeExpressions(List<IScriptExpression> expressions) {
        for (IScriptExpression expression : expressions) {
            if (expression.getNewFieldNames() != null && expression.getNewFieldNames().size() == 1) {
                String newFieldName = expression.getNewFieldNames().iterator().next();
                varName2Scripts.put(newFieldName, expression);
                varName2DependentFields.put(newFieldName, expression.getDependentFields());
            }
        }
        List<IScriptExpression> beforeExpressions = new ArrayList<>();
        Set<String> varNames = getDependentFields();
        for (String varName : varNames) {
            beforeExpressions.addAll(addBeforeExpression(varName));
        }
        Collections.sort(beforeExpressions, new Comparator<IScriptExpression>() {
            @Override public int compare(IScriptExpression o1, IScriptExpression o2) {
                List<String> varNames1 = o1.getDependentFields();
                List<String> varNames2 = o2.getDependentFields();
                for (String varName : varNames1) {
                    if (o2.getNewFieldNames() != null && o2.getNewFieldNames().contains(varName)) {
                        return 1;
                    }
                }
                for (String varName : varNames2) {
                    if (o1.getNewFieldNames() != null && o1.getNewFieldNames().contains(varName)) {
                        return -1;
                    }
                }
                return 0;
            }
        });
        return beforeExpressions;
    }

    protected List<IScriptExpression> addBeforeExpression(String varName) {
        IScriptExpression scriptExpression = this.varName2Scripts.get(varName);
        if (scriptExpression == null) {
            return new ArrayList<>();
        }
        List<IScriptExpression> list = new ArrayList<>();
        list.add(scriptExpression);
        //this.beforeScriptExpressions.add(scriptExpression);
        List<String> fields = scriptExpression.getDependentFields();
        if (fields != null) {
            for (String fieldName : fields) {
                List<IScriptExpression> dependents = addBeforeExpression(fieldName);
                if (dependents == null) {
                    return null;
                } else {
                    list.addAll(dependents);
                }
            }
        }
        return list;
    }

    public Set<String> getDependentFields() {
        return rule.getDependentFields();
    }

    public Set<String> getNewFieldNames() {
        Set<String> newFieldNames = new HashSet<>();
        for (IScriptExpression then : thenScriptExpressionList) {
            newFieldNames.addAll(then.getNewFieldNames());
        }
        return newFieldNames;
    }

    public Rule getRule() {
        return rule;
    }

    public List<IScriptExpression> getThenScriptExpressionList() {
        return thenScriptExpressionList;
    }

}
