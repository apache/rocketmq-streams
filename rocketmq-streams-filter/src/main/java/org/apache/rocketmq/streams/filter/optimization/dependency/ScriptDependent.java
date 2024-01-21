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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.script.CaseFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.script.function.impl.eval.EvalFunction;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class ScriptDependent {
    private static Set<String> udtfNames = new HashSet<>();

    static {
        for (int i = 0; i < 100; i++) {
            udtfNames.add(FunctionType.UDTF.getName() + "" + i);
        }
    }

    protected transient Map<String, IScriptExpression> varName2Scripts = new HashMap<>();
    protected transient Map<String, List<String>> varName2DependentFields = new HashMap<>();
    protected transient String namespace;
    protected transient boolean hasExecFunction = false;

    public ScriptDependent(String namespace, String scriptValue) {
        this.namespace = namespace;
        FunctionScript functionScript = new FunctionScript(scriptValue);
        functionScript.init();
        init(functionScript.getScriptExpressions());

    }

    public ScriptDependent(FunctionScript functionScript) {
        this.namespace = functionScript.getNameSpace();
        init(functionScript.getScriptExpressions());
    }

    public ScriptDependent(List<IScriptExpression> scriptExpressions) {
        init(scriptExpressions);
    }

    private void init(List<IScriptExpression> scriptExpressions) {
        for (IScriptExpression expression : scriptExpressions) {
            if (expression.getNewFieldNames() != null && expression.getNewFieldNames().size() == 1) {
                String newFieldName = expression.getNewFieldNames().iterator().next();
                if (CaseFunction.isCaseFunction(expression.getFunctionName())) {
                    String expressionStr = FunctionUtils.getConstant(((IScriptParamter) expression.getScriptParamters().get(0)).getScriptParameterStr());
                    varName2Scripts.put(newFieldName, expression);
                    Rule rule = ExpressionBuilder.createRule("tmp", "tmp", expressionStr);
                    varName2DependentFields.put(newFieldName, new ArrayList<>(rule.getDependentFields()));
                    continue;
                }
                varName2Scripts.put(newFieldName, expression);
                varName2DependentFields.put(newFieldName, expression.getDependentFields());

            }
            if (EvalFunction.isFunction(expression.getFunctionName())) {
                hasExecFunction = true;
            }
        }
    }

    /**
     * trace root field dependent in this script
     *
     * @param varName
     * @return
     */
    public Set<String> traceaField(String varName, AtomicBoolean isBreak, List<IScriptExpression> commonExpressions) {
        if (hasExecFunction) {
            isBreak.set(true);
            return null;
        }
        Set<String> hasTraceFieldNames = new HashSet<>();
        hasTraceFieldNames.add(varName);
        Set<String> vars = traceaField(varName, hasTraceFieldNames, true, isBreak, commonExpressions);
        return vars;
    }

    /**
     * trace root field dependent in this script
     *
     * @param varName
     * @return
     */
    public Set<String> traceaField(String varName, Set<String> hasTraceFieldNames, boolean isFirst, AtomicBoolean isBreak, List<IScriptExpression> commonExpressions) {

        if ("null".equals(varName)) {
            return new HashSet<>();
        }
        Set<String> fields = new HashSet<>();

        if (hasTraceFieldNames.contains(varName) && !isFirst) {
            fields.add(varName);
            return fields;
        }
        List<String> depenentFields = varName2DependentFields.get(varName);
        if (depenentFields == null) {
            fields.add(varName);
            return fields;
        }
        IScriptExpression scriptExpression = this.varName2Scripts.get(varName);
        if (scriptExpression != null) {
            commonExpressions.add(scriptExpression);
        }
        Set<String> depenentFieldSet = new HashSet<>(depenentFields);
        for (String newFieldName : depenentFieldSet) {
            if (newFieldName.equals(varName)) {
                fields.add(varName);
                continue;
            }
            if (udtfNames.contains(newFieldName)) {
                isBreak.set(true);
                fields.add(varName);
                continue;
            }
            Set<String> dependentFields = traceaField(newFieldName, hasTraceFieldNames, false, isBreak, commonExpressions);
            fields.addAll(dependentFields);
        }
        return fields;
    }

    public List<IScriptExpression> getDependencyExpression(String varName) {
        IScriptExpression scriptExpression = this.varName2Scripts.get(varName);
        if (scriptExpression == null) {
            return new ArrayList<>();
        }
        List<IScriptExpression> list = new ArrayList<>();
        list.add(scriptExpression);
        List<String> fields = scriptExpression.getDependentFields();
        if (fields != null) {
            for (String fieldName : fields) {
                List<IScriptExpression> dependents = getDependencyExpression(fieldName);
                if (dependents == null) {
                    return null;
                } else {
                    list.addAll(dependents);
                }
            }
        }
        Collections.sort(list, new Comparator<IScriptExpression>() {
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
        return list;
    }
}