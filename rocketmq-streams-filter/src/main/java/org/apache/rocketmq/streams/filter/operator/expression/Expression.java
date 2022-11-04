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
package org.apache.rocketmq.streams.filter.operator.expression;

import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.optimization.HomologousVar;
import org.apache.rocketmq.streams.common.optimization.LikeRegex;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.function.expression.ExpressionFunction;
import org.apache.rocketmq.streams.filter.function.expression.InFunction;
import org.apache.rocketmq.streams.filter.function.expression.IsNotNull;
import org.apache.rocketmq.streams.filter.function.expression.IsNull;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.IConfigurableAction;
import org.apache.rocketmq.streams.filter.operator.var.ContextVar;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.function.model.FunctionConfigure;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Expression<T> extends BasedConfigurable implements IConfigurable, IConfigurableAction<Boolean>, Serializable, IStreamOperator<IMessage, Boolean> {
    public static final String TYPE = "express";
    private static final long serialVersionUID = 4610495074511059465L;
    private static final Logger LOGGER = LoggerFactory.getLogger(Expression.class);
    private static final String FIELD_COMPARE = "field";
    private static final String AES_KEY = "baicheng.cbc";
    private String varName;
    private String functionName;
    protected transient HomologousVar homologousVar;
    @SuppressWarnings("rawtypes")
    private DataType dataType = new StringDataType(String.class);
    protected T value;

    public Expression copy() {
        Expression expression = new Expression();
        copy(expression);
        return expression;
    }

    public void copy(Expression expression) {
        expression.setNameSpace(getNameSpace());
        expression.setType(getType());
        expression.setConfigureName(getConfigureName());
        expression.setVarName(varName);
        expression.setFunctionName(functionName);
        expression.setDataType(dataType);
        expression.setValue(value);
        expression.setPrivateDatas(privateDatas);
        expression.setAesFlag(aesFlag);
    }

    // 前端处理使用
    private String dataTypestr;

    // 表达式中的关键词 正则之前进行字符串判断
    private String keyword;

    // 表达式的值是否加密 0未加密 1加密
    private int aesFlag = 0;

    /**
     * 表达式是否是字段之间的比较，dataTypestr为field时为true
     */
    private boolean fieldFlag = false;
    private volatile boolean initFlag = false;

    protected transient ExpressionFunction expressionFunction;
    protected transient Var var;

    public Expression() {
        setType(TYPE);
    }

    protected static RegexFunction regexFunction = new RegexFunction();
    protected static LikeFunction likeFunction = new LikeFunction();
    protected static InFunction inFunction = new InFunction();
    protected static IsNotNull isNotNull = new IsNotNull();
    protected static IsNull isNull = new IsNull();
    protected static transient Map<String, ExpressionFunction> cache = new HashMap<>();

    @Override
    public Boolean doMessage(IMessage message, AbstractContext context) {
        try {
            Boolean isMatch = null;
            if (this.homologousVar != null) {
                isMatch = context.matchFromHomologousCache(context.getMessage(), this.homologousVar);
            }
            if (isMatch != null) {
                return isMatch;
            }
            isMatch = context.matchFromCache(context.getMessage(), this);
            if (isMatch != null) {
                return isMatch;
            }
            long startTime = System.currentTimeMillis();

            isMatch = executeFunctionDirectly(message, context);
            long cost = System.currentTimeMillis() - startTime;
            long timeout = 10;
            if (ComponentCreator.getProperties().getProperty(ConfigureFileKey.MONITOR_SLOW_TIMEOUT) != null) {
                timeout = Long.parseLong(ComponentCreator.getProperties().getProperty(ConfigureFileKey.MONITOR_SLOW_TIMEOUT));
            }
            if (cost > timeout) {
                LOGGER.debug("SLOW-" + cost + "----" + this.toString() + PrintUtil.LINE + "the var value is " + message.getMessageBody().getString(varName));
            }
            return isMatch;
        } catch (Exception e) {
            LOGGER.error("SLOW-" + this.toString() + PrintUtil.LINE + "the var value is " + message.getMessageBody().getString(varName), e);
            throw e;
        }
    }

    private transient LikeRegex likeRegex;

    @SuppressWarnings("unchecked")
    protected Boolean executeFunctionDirectly(IMessage message, AbstractContext context) {

        ExpressionFunction function = getExpressionFunction(getFunctionName(), message, context, this);

        if (function == null) {
            return null;
        }
        /**
         * 表达式新增fieldFlag 如：src_port=desc_port fieldFlag为true时，设置value为对应的字段值
         */
        if (fieldFlag && !initFlag) {
            synchronized (Expression.class) {
                if (fieldFlag && !initFlag) {
                    Object varObject = var.doMessage(message, context);
                    this.value = (T) String.valueOf(varObject).trim();
                }
            }
        }
        boolean result = false;
        try {

            if (RegexFunction.isRegex(functionName)) {
                String varValue = message.getMessageBody().getString(this.varName);
                String regex = (String) this.value;
                return varValue != null && StringUtil.matchRegex(varValue, regex);
            }
            if (LikeFunction.isLikeFunciton(functionName)) {
                String varValue = message.getMessageBody().getString(this.varName);
                String like = (String) this.value;
                if (this.likeRegex == null) {
                    this.likeRegex = new LikeRegex(like);
                }
                return varValue != null && likeRegex.match(varValue);
            }
            if (var == null) {
                var = new ContextVar();
                ((ContextVar) var).setFieldName(this.varName);
            }
            result = function.doFunction(message, context, this);
        } catch (Exception e) {
            LOGGER.error(
                "Expression doAction function.doFunction error,rule is: " + getConfigureName() + " ,express is:"
                    + getConfigureName(), e);
            throw e;
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    @Override
    public String toJson() {
        String valueStr = String.valueOf(value);
        try {
            // 适配fastjson的bug，parseArray时会对"-"进行分割
            if (dataType instanceof ListDataType && valueStr.contains("-") && valueStr.contains(",")) {
                this.value = (T) value;
            } else {
                // value很多时候会是String类型，这样dataType.toDataJson会报错，所以先转为dataType类型,
                // list类型JsonArray.tojson会报错，不做处理（[pfile_path_rule,proc_vul_exploit_rce_rule]）
                if (!(dataType instanceof ListDataType)) {
                    this.value = (T) dataType.getData(valueStr);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(IConfigurableService.CLASS_NAME, this.getClass().getName());
        jsonObject.put("varName", varName);
        jsonObject.put("functionName", functionName);

        if (FIELD_COMPARE.equals(dataTypestr)) {
            fieldFlag = true;
        }

        // 如果是String，且不是字段比较，则加密
        if (!fieldFlag) {
            if (dataType instanceof StringDataType) {
                try {
                    jsonObject.put("value", AESUtil.aesEncrypt(valueStr, AES_KEY));
                    jsonObject.put("aesFlag", 1);
                } catch (Exception e) {
                    jsonObject.put("value", dataType.toDataJson(value));
                }
            } else {
                jsonObject.put("value", dataType.toDataJson(value));
            }
        } else {
            jsonObject.put("value", valueStr);
        }
        jsonObject.put("dataType", dataType.toJson());
        jsonObject.put("keyword", keyword == null ? "" : keyword.toLowerCase().trim());
        jsonObject.put("fieldFlag", fieldFlag);

        getJsonValue(jsonObject);
        return jsonObject.toJSONString();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void toObject(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        this.varName = jsonObject.getString("varName");
        this.functionName = jsonObject.getString("functionName");
        String dataTypeJson = jsonObject.getString("dataType");
        this.dataType = DataTypeUtil.createDataType(dataTypeJson);
        String valueJson = jsonObject.getString("value");
        this.aesFlag = jsonObject.getIntValue("aesFlag");
        this.fieldFlag = jsonObject.getBooleanValue("fieldFlag");

        // 前端显示用
        String dataTypestr = "";
        try {
            if (dataType != null) {
                dataTypestr = MetaDataField.getDataTypeStrByType(dataType);
            }
        } catch (Exception e) {
            dataTypestr = "String";
        }

        if (!fieldFlag) {
            if (dataType instanceof StringDataType) {
                // value经过加密 需要解密
                if (aesFlag == 1) {
                    try {
                        this.value = (T) AESUtil.aesDecrypt(valueJson, AES_KEY);
                    } catch (Exception e) {
                        this.value = (T) this.dataType.getData(valueJson);
                    }
                } else {
                    this.value = (T) this.dataType.getData(valueJson);
                }
            } else {
                this.value = (T) this.dataType.getData(valueJson);
            }
        } else {
            dataTypestr = FIELD_COMPARE;
            this.value = (T) valueJson;
        }

        this.dataTypestr = dataTypestr;
        this.keyword =
            jsonObject.getString("keyword") == null ? "" : jsonObject.getString("keyword").toLowerCase().trim();
        setJsonValue(jsonObject);
    }

    protected void getJsonValue(JSONObject jsonObject) {
    }

    protected void setJsonValue(JSONObject jsonObject) {
    }

    public Set<String> getDependentFields(Map<String, Expression> expressionMap) {
        Set<String> set = new HashSet<>();
        if (StringUtil.isNotEmpty(varName) && !ContantsUtil.isContant(varName) && !FunctionUtils.isNumber(varName) && !FunctionUtils.isBoolean(varName)) {
            set.add(varName);
        }
        return set;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public DataType getDataType() {
        return dataType;
    }

    public void setDataType(DataType dataType) {
        this.dataType = dataType;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    @Override
    public boolean init() {
        return true;
    }

    /**
     * 是否支持快速匹配，快速匹配的意思是var不需要io
     *
     * @param expression
     * @param context
     * @param rule
     * @return
     */
    public boolean supportQuickMatch(Expression expression, RuleContext context, Rule rule) {
        String varName = expression.getVarName();
        Var var = context.getVar(rule.getConfigureName(), varName);
        if (var == null || var.canLazyLoad()) {
            return false;
        }
        return true;
    }

    /**
     * 合法性验证
     *
     * @return
     */
    public boolean volidate() {
        return !StringUtil.isEmpty(varName) && !StringUtil.isEmpty(functionName) && dataType != null;
    }

    public String getDataTypestr() {
        return dataTypestr;
    }

    public void setDataTypestr(String dataTypestr) {
        this.dataTypestr = dataTypestr;
        if (FIELD_COMPARE.equals(dataTypestr)) {
            fieldFlag = true;
        }

        this.dataType = MetaDataField.getDataTypeByStr(dataTypestr);
    }

    public String getKeyword() {
        return keyword;
    }

    public void setKeyword(String keyword) {
        if (keyword != null) {
            keyword = keyword.toLowerCase().trim();
        }
        this.keyword = keyword;
    }

    public int getAesFlag() {
        return aesFlag;
    }

    public void setAesFlag(int aesFlag) {
        this.aesFlag = aesFlag;
    }
    public String toExpressionString(Map<String, Expression> name2Expression,
        String... expressionNamePrefixs) {
        return toExpressionString(name2Expression,0,expressionNamePrefixs);
    }
    /**
     * 返回expressCode
     *
     * @return
     */
    @SuppressWarnings("rawtypes")
    public String toExpressionString(Map<String, Expression> name2Expression,int  blackCount,
        String... expressionNamePrefixs) {
        String dataType = null;
        String result = "(" + varName + "," + getFunctionName() + ",";
        if (!getDataType().matchClass(String.class)) {
            dataType = getDataType().getDataTypeName();
            result += dataType + ",";
        }
        String value = getDataType().toDataJson(getValue());
        if (getDataType().matchClass(String.class) && needContains(value)) {
            value = "'" + value + "'";
        }
        result += value + ")";
        for(int i=0;i<blackCount;i++){
            result="&nbsp;&nbsp;&nbsp;"+result;
        }

        return result;
    }

    @Override
    public String toString() {
        String dataType = null;
        String result = varName + " " + getFunctionName() + " ";
//        if (!getDataType().matchClass(String.class)) {
//            dataType = getDataType().getDataTypeName();
//            result += dataType + ",";
//        }
        String value = getDataType().toDataJson(getValue());
        if (getDataType().matchClass(String.class) && needContains(value)) {
            value = "'" + value + "'";
        }
        result += value;

        return result;
    }

    private boolean needContains(String value) {
        return value.contains("(") || value.contains(",");
    }

    public HomologousVar getHomologousVar() {
        return homologousVar;
    }

    public void setHomologousVar(HomologousVar homologousVar) {
        this.homologousVar = homologousVar;
    }

    protected ExpressionFunction getExpressionFunction(String functionName, Object... paras) {
        try {
            FunctionConfigure fc = ScriptComponent.getInstance().getFunctionService().getFunctionConfigure(functionName, paras);
            if (fc == null) {
                return null;
            }
            return (ExpressionFunction) fc.getBean();
        } catch (Exception e) {
            LOGGER.error("RuleContext getExpressionFunction error,name is: " + functionName, e);
            return null;
        }

    }

    public Var getVar() {
        return var;
    }

    public void setVar(Var var) {
        this.var = var;
    }

}
