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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.optimization.CompileScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 一个函数，如a=now();就是一个表达式 这里是函数真正执行的地方
 */
@SuppressWarnings("rawtypes")
public class ScriptExpression implements IScriptExpression {

    private static Log LOG = LogFactory.getLog(ScriptExpression.class);

    private String newFieldName;

    private String expressionStr;

    private String functionName;

    private List<IScriptParamter> parameters;

    private Long groupId;

    protected transient volatile CompileScriptExpression compileScriptExpression;
    private transient static ICache<String, Boolean> cache = new SoftReferenceCache<>();

    @Override
    public Object executeExpression(IMessage message, FunctionContext context) {

        try {
            if (StringUtil.isEmpty(functionName)) {
                Object value = parameters.get(0).getScriptParamter(message, context);
                setValue2Var(message, context, newFieldName,
                    FunctionUtils.getValue(message, context, value.toString()));
                return value;
            }
            Object value = null;
            if (compileScriptExpression != null) {
                value = compileScriptExpression.execute(message, context);
            } else {
                value = execute(message, context);
            }

            //monitor.setResult(value);
            //monitor.endMonitor();
            //if (monitor.isSlow()) {
            //    monitor.setSampleData(context).put("script_info", expressionStr);
            //}
            return value;
        } catch (Exception e) {
            e.printStackTrace();
            IMonitor monitor = null;
            if (StringUtil.isEmpty(functionName)) {
                monitor = context.getCurrentMonitorItem("scripts").createChildren("operator");
            } else {
                monitor = context.getCurrentMonitorItem("scripts").createChildren(functionName);
            }
            monitor.occureError(e, "operator expression execute error " + expressionStr, e.getMessage());
            monitor.setSampleData(context).put("script_info", expressionStr);
            throw new RuntimeException(e);
        }

    }

    private ScriptComponent scriptComponent = ScriptComponent.getInstance();

    public Object execute(IMessage message, FunctionContext context) {
        Object[] ps = null;
        FunctionConfigure functionConfigure = null;
        if (cache.get(functionName) != null && cache.get(functionName)) {
            ps = createParameters(message, context);
            functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(functionName, ps);
        }

        if (functionConfigure == null) {
            ps = createParameters(message, context, true, message, context);
            functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(functionName, ps);
        }

        if (functionConfigure == null) {
            cache.put(functionName, true);
            ps = createParameters(message, context, false, null);
            functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(functionName, ps);
        }
        if (functionConfigure == null) {
            System.out.println("");
            Object[] tmp = createParameters(message, context, false, null);
            functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(functionName, ps);
            System.out.println("");
        }
        Object value = functionConfigure.execute(ps);
            compileScriptExpression = new CompileScriptExpression(this, functionConfigure);
        if (StringUtil.isNotEmpty(newFieldName) && value != null) {
            setValue2Var(message, context, newFieldName, value);
            //message.getMessageBody().put(newFieldName, value);
        }
        return value;
    }

    protected transient Boolean hasSubField = null;

    public void setValue2Var(IMessage message, AbstractContext context, String newFieldName, Object value) {
        if (newFieldName == null || value == null) {
            return;
        }
        if (newFieldName.indexOf(".") == -1) {
            message.getMessageBody().put(newFieldName, value);
            return;
        }
        int lastIndex = newFieldName.lastIndexOf(".");
        String objectName = newFieldName.substring(0, lastIndex);
        Object object = ReflectUtil.getBeanFieldOrJsonValue(message.getMessageBody(), objectName);
        String fieldName = newFieldName.substring(lastIndex + 1);
        if (object == null) {
            message.getMessageBody().put(newFieldName, value);
            return;
        }
        ReflectUtil.setBeanFieldValue(object, fieldName, value);
    }

    @Override
    public List<IScriptParamter> getScriptParamters() {
        return this.parameters;
    }

    private Object[] createParameters(IMessage message, FunctionContext context) {
        return createParameters(message, context, false, null);
    }

    /***
     * 创建参数，必要时加前缀
     * @param message
     * @param context
     * @param firstParas
     * @return
     */
    private Object[] createParameters(IMessage message, FunctionContext context, boolean needContext,
                                      Object... firstParas) {

        Object[] paras;
        if (this.parameters == null) {
            if (firstParas != null) {
                return firstParas;
            }
            return new Object[0];
        }
        int firstLen = firstParas == null ? 0 : firstParas.length;
        int length = this.parameters.size() + firstLen;
        paras = new Object[length];
        int i = 0;
        for (; i < firstLen; i++) {
            paras[i] = firstParas[i];
        }
        for (; i < length; i++) {
            if (needContext) {
                paras[i] = parameters.get(i - firstLen).getScriptParamter(message, context);
            } else {
                Object value = parameters.get(i - firstLen).getScriptParamter(message, context);
                if (value == null) {
                    paras[i] = null;
                }
                if (String.class.isInstance(value)) {
                    String str = (String)value;
                    Object object = FunctionUtils.getValue(message, context, str);
                    paras[i] = object;
                } else {
                    paras[i] = value;
                }
            }

        }
        return paras;
    }

    @Override
    public List<String> getDependentFields() {
        List<String> fieldNames = new ArrayList<>();
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

    @Override
    public Set<String> getNewFieldNames() {
        Set<String> set = new HashSet<>();
        if (StringUtil.isNotEmpty(newFieldName)) {
            set.add(newFieldName);
        }
        return set;
    }

    public String getExpressionStr() {
        return expressionStr;
    }

    public void setExpressionStr(String expressionStr) {
        this.expressionStr = expressionStr;
    }

    @Override
    public String getFunctionName() {
        return functionName;
    }

    @Override
    public String getExpressionDescription() {
        return expressionStr;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public List<IScriptParamter> getParameters() {
        return parameters;
    }

    public void setParameters(List<IScriptParamter> parameters) {
        this.parameters = parameters;
    }

    public Long getGroupId() {
        return groupId;
    }

    public void setGroupId(Long groupId) {
        this.groupId = groupId;
    }

    @Override
    public Object getScriptParamter(IMessage message, FunctionContext context) {
        Object value = this.executeExpression(message, context);
        context.putValue(this.getScriptParameterStr(), value);
        if (value == null) {
            return null;
        }
        if (FunctionUtils.isNumber(value.toString())) {
            return value;
        } else {
            return "'" + value + "'";
        }

    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        if (StringUtil.isNotEmpty(newFieldName)) {
            stringBuilder.append(newFieldName);
            stringBuilder.append("=");
        }
        if (StringUtil.isNotEmpty(functionName)) {
            stringBuilder.append(functionName);
            stringBuilder.append("(");
        }
        boolean isfirst = true;
        if (this.parameters != null) {
            for (IScriptParamter paramter : parameters) {
                if (isfirst) {
                    isfirst = false;
                } else {
                    stringBuilder.append(",");
                }
                stringBuilder.append(paramter);
            }
        }
        if (StringUtil.isNotEmpty(functionName)) {
            stringBuilder.append(")");
        }
        return stringBuilder.toString();
    }

    @Override
    public String getScriptParameterStr() {
        return expressionStr;
    }

    public String getNewFieldName() {
        return newFieldName;
    }

    public void setNewFieldName(String newFieldName) {
        this.newFieldName = newFieldName;
    }

}
