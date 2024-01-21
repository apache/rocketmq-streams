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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.HomologousVar;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.optimization.compile.CompileParameter;
import org.apache.rocketmq.streams.script.optimization.compile.CompileScriptExpression;
import org.apache.rocketmq.streams.script.optimization.performance.IScriptOptimization;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 一个函数，如a=now();就是一个表达式 这里是函数真正执行的地方
 */
@SuppressWarnings("rawtypes")
public class ScriptExpression implements IScriptExpression {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptExpression.class);
    private final static ICache<String, Boolean> cache = new SoftReferenceCache<>();
    protected transient Boolean isMultiField;
    protected transient HomologousVar homologousVar;
    protected transient volatile CompileScriptExpression compileScriptExpression;
    protected transient volatile CompileParameter compileParameter;
    protected transient Boolean hasSubField = null;
    private String newFieldName;
    private String expressionStr;
    private String functionName;
    private List<IScriptParamter> parameters;
    private Long groupId;
    private ScriptComponent scriptComponent = ScriptComponent.getInstance();

    @Override
    public Object executeExpression(IMessage message, FunctionContext context) {
        String configName = message.getHeader().getPipelineName();
        try {
            if (isMultiField == null && newFieldName != null) {
                isMultiField = newFieldName.contains(".");
            }
            Boolean isMatch = null;
            if (this.homologousVar != null) {
                isMatch = context.matchFromHomologousCache(message, this.homologousVar);
            }
            if (isMatch != null) {
                setValue2Var(message, context, newFieldName, isMatch);
                return isMatch;
            }
            isMatch = context.matchFromCache(message, this);
            if (isMatch != null) {
                setValue2Var(message, context, newFieldName, isMatch);
                return isMatch;
            }

            if (StringUtil.isEmpty(functionName)) {
                if (compileParameter == null) {
                    compileParameter = new CompileParameter(parameters.get(0), false);
                }
                Object value = compileParameter.getValue(message, context);
                setValue2Var(message, context, newFieldName, value);
                return value;
            }
            long startTime = System.currentTimeMillis();
            Object value = null;
            if (compileScriptExpression != null) {
                value = compileScriptExpression.execute(message, context);
            } else {
                value = execute(message, context);
            }
            long cost = System.currentTimeMillis() - startTime;
            long timeout = 1000;
            if (SystemContext.getProperty(ConfigurationKey.MONITOR_SLOW_TIMEOUT) != null) {
                timeout = Long.parseLong(SystemContext.getProperty(ConfigurationKey.MONITOR_SLOW_TIMEOUT));
            }
            if (cost > timeout) {
                LOGGER.warn("[{}][{}] Expression_Exec_Slow_({})_Cost({})", IdUtil.instanceId(), configName, this.expressionStr, cost);

            }
            return value;
        } catch (Exception e) {
            Object[] ps = createParameters(message, context, false, null);
            throw new RuntimeException("[" + this.expressionStr + "]--[" + MapKeyUtil.createKeyFromObject(ps) + "]", e);
        }

    }

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
            ps = createParameters(message, context, false, null);
            functionConfigure = scriptComponent.getFunctionService().getFunctionConfigure(functionName, ps);
            if (functionConfigure != null) {
                cache.put(functionName, true);
            }
        }
        if (functionConfigure == null) {
            String varValue = "";
            if (this.getScriptParamters() != null && this.getScriptParamters().size() > 0) {
                varValue = IScriptOptimization.getParameterValue(this.getParameters().get(0));
                varValue = message.getMessageBody().getString(varValue);
            }
            String configName = message.getHeader().getPipelineName();
            throw new RuntimeException("[" + this.expressionStr + "]--[" + MapKeyUtil.createKeyFromObject(ps) + "]");
        }
        Object value = executeFunctionConfigue(message, context, functionConfigure, ps);
        compileScriptExpression = new CompileScriptExpression(this, functionConfigure);
        if (StringUtil.isNotEmpty(newFieldName) && value != null) {
            setValue2Var(message, context, newFieldName, value);
        }
        return value;
    }

    public Object executeFunctionConfigue(IMessage message, FunctionContext context, FunctionConfigure configure,
        Object[] ps) {
        Object value = configure.execute(ps);

        if (configure.isUserDefinedUDTF()) {
            List<Map<String, Object>> rows = (List<Map<String, Object>>) value;
            context.openSplitModel();
            boolean needFlush = message.getHeader().isNeedFlush();
            context.openSplitModel();
            for (int i = 0; i < rows.size(); i++) {
                JSONObject jsonObject = new JSONObject();
                jsonObject.putAll(message.getMessageBody());
                Map subJsonObject = (Map) rows.get(i);
                jsonObject.putAll(subJsonObject);
                IMessage copyMessage = message.deepCopy();
                copyMessage.setMessageBody(jsonObject);
                if (i < rows.size() - 1) {
                    copyMessage.getHeader().setNeedFlush(false);
                } else {
                    copyMessage.getHeader().setNeedFlush(needFlush);
                }
                context.addSplitMessages(copyMessage);
            }
            return null;
        }
        return value;
    }

    public void setValue2Var(IMessage message, AbstractContext context, String newFieldName, Object value) {
        if (newFieldName == null) {
            return;
        }
        if (!isMultiField) {
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
                if (value instanceof String) {
                    String str = (String) value;
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

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    @Override
    public String getExpressionDescription() {
        return expressionStr;
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

    public HomologousVar getHomologousVar() {
        return homologousVar;
    }

    public void setHomologousVar(HomologousVar homologousVar) {
        this.homologousVar = homologousVar;
    }
}
