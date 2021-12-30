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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.optimization.HomologousVar;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;

public class CommonExpression {
    protected String varName;//var name in stage
    protected String value;//expression value eg:regex,like string
    protected boolean isRegex;//if regex return true else false
    protected String sourceVarName;//the var name in source
    protected Integer index;//set by HomologousCompute, is cache result bitset index
    protected List<IScriptExpression> scriptExpressions = new ArrayList<>();

    protected IScriptExpression scriptExpression;
    protected Expression expression;

    public CommonExpression(IScriptExpression expression) {
        if (!support(expression)) {
            return;
        }
        this.scriptExpression = expression;
        IScriptParamter varParameter = (IScriptParamter) expression.getScriptParamters().get(0);
        IScriptParamter regexParameter = (IScriptParamter) expression.getScriptParamters().get(1);
        String regex = IScriptOptimization.getParameterValue(regexParameter);
        String varName = IScriptOptimization.getParameterValue(varParameter);
        this.varName = varName;
        this.value = regex;
        this.isRegex = true;

    }

    public CommonExpression(Expression expression) {
        if (!support(expression)) {
            return;
        }
        this.expression = expression;
        if (RegexFunction.isRegex(expression.getFunctionName())) {
            value = (String) expression.getValue();
            varName = expression.getVarName();
            isRegex = true;
        }
        if (LikeFunction.isLikeFunciton(expression.getFunctionName())) {
            value = (String) expression.getValue();
            varName = expression.getVarName();
            isRegex = false;
        }
    }

    public static boolean support(IScriptExpression expression) {
        if (GroupScriptExpression.class.isInstance(expression)) {
            return false;
        }
        if (expression.getFunctionName() == null) {
            return false;
        }
        if (expression.getScriptParamters() == null || expression.getScriptParamters().size() != 2) {
            return false;
        }
        if (org.apache.rocketmq.streams.script.function.impl.string.RegexFunction.isRegexFunction(expression.getFunctionName())) {
            return true;
        }

        return false;
    }

    public static boolean support(Expression expression) {
        if (expression.getFunctionName() == null) {
            return false;
        }
        if (RegexFunction.isRegex(expression.getFunctionName()) || LikeFunction.isLikeFunciton(expression.getFunctionName())) {
            return true;
        }
        return false;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public boolean isRegex() {
        return isRegex;
    }

    public void setRegex(boolean regex) {
        isRegex = regex;
    }

    public List<IScriptExpression> getScriptExpressions() {
        return scriptExpressions;
    }

    public void setScriptExpressions(
        List<IScriptExpression> scriptExpressions) {
        this.scriptExpressions = scriptExpressions;
    }

    public boolean init() {
        Collections.sort(this.scriptExpressions, new Comparator<IScriptExpression>() {
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
        ScriptDependent scriptDependent = new ScriptDependent(this.scriptExpressions);
        Set<String> varNames = scriptDependent.traceaField(this.varName, new AtomicBoolean(false), new ArrayList<>());
        if (CollectionUtil.isEmpty(varNames)) {
            this.sourceVarName = varName;
        } else {
            if (varNames.size() > 1) {
                return false;
            }
            this.sourceVarName = varNames.iterator().next();

        }
        return true;
    }

    public void addPreviewScriptDependent(List<IScriptExpression> scriptExpressions) {
        if (scriptExpressions == null) {
            return;
        }

        this.scriptExpressions.addAll(scriptExpressions);

    }

    public String getSourceVarName() {
        return sourceVarName;
    }

    public void setSourceVarName(String sourceVarName) {
        this.sourceVarName = sourceVarName;
    }

    public void addHomologousVarToExpression() {
        HomologousVar homologousVar = new HomologousVar();
        homologousVar.setSourceVarName(this.sourceVarName);
        homologousVar.setIndex(index);
        homologousVar.setVarName(this.varName);
        if (scriptExpression != null && ScriptExpression.class.isInstance(scriptExpression)) {
            ScriptExpression scriptExpressionImp = (ScriptExpression) scriptExpression;
            scriptExpressionImp.setHomologousVar(homologousVar);
        }
        if (expression != null) {
            expression.setHomologousVar(homologousVar);
        }
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

}
