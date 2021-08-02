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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

/**
 * 条件函数，支持if elseif else 每一个部分又是用IScriptExpression来表达。 执行流程，如果if成功执行then，循环执行elseif，最后执行else
 */
public class GroupScriptExpression implements IScriptExpression {

    private IScriptExpression ifExpresssion;

    private List<IScriptExpression> thenExpresssions;

    private List<IScriptExpression> elseExpressions;

    private String scriptParameterStr;

    protected List<GroupScriptExpression> elseIfExpressions = new ArrayList<>();

    private static final String TAB = FunctionParser.TAB;

    @Override
    public Object getScriptParamter(IMessage message, FunctionContext context) {
        return this.executeExpression(message, context);
    }

    @Override
    public String getScriptParameterStr() {
        return scriptParameterStr;
    }

    @Override
    public Object executeExpression(IMessage channelMessage, FunctionContext context) {
        Boolean result = (Boolean)ifExpresssion.executeExpression(channelMessage, context);
        Object value = null;
        if (result) {
            if (thenExpresssions != null) {
                for (IScriptExpression scriptExpression : thenExpresssions) {
                    value = scriptExpression.executeExpression(channelMessage, context);
                }
            }
            return value;
        }
        if (elseIfExpressions != null && elseIfExpressions.size() > 0) {
            for (int i = elseIfExpressions.size() - 1; i >= 0; i--) {
                GroupScriptExpression expression = elseIfExpressions.get(i);
                boolean success = (Boolean)expression.ifExpresssion.executeExpression(channelMessage, context);
                if (success) {
                    if (expression.thenExpresssions != null) {
                        for (IScriptExpression scriptExpression : expression.thenExpresssions) {
                            value = scriptExpression.executeExpression(channelMessage, context);
                        }
                    }
                    return value;
                }
            }
        }
        if (elseExpressions != null) {
            for (IScriptExpression scriptExpression : elseExpressions) {
                value = scriptExpression.executeExpression(channelMessage, context);
            }
        }
        return value;
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
        if (parameters != null && parameters.size() > 0) {
            for (IScriptParamter scriptParamter : parameters) {
                List<String> names = scriptParamter.getDependentFields();
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

    public void setIfExpresssion(IScriptExpression ifExpresssion) {
        this.ifExpresssion = ifExpresssion;
    }

    public void setThenExpresssions(List<IScriptExpression> thenExpresssions) {
        this.thenExpresssions = thenExpresssions;
    }

    public void setElseExpressions(List<IScriptExpression> elseExpressions) {
        this.elseExpressions = elseExpressions;
    }

    public void setScriptParameterStr(String scriptParameterStr) {
        this.scriptParameterStr = scriptParameterStr;
    }

    public List<GroupScriptExpression> getElseIfExpressions() {
        return elseIfExpressions;
    }

    public void setElseIfExpressions(List<GroupScriptExpression> elseIfExpressions) {
        this.elseIfExpressions = elseIfExpressions;
    }

    public List<IScriptExpression> getElseExpressions() {
        return elseExpressions;
    }

}
