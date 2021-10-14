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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.AESUtil;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.function.expression.ExpressionFunction;
import org.apache.rocketmq.streams.filter.function.expression.InFunction;
import org.apache.rocketmq.streams.filter.function.expression.IsNotNull;
import org.apache.rocketmq.streams.filter.function.expression.IsNull;
import org.apache.rocketmq.streams.filter.function.expression.LikeFunction;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.monitor.Monitor;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.IConfigurableAction;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class Expression<T> extends BasedConfigurable
    implements IConfigurable, IConfigurableAction<Boolean>, Serializable {
    public static final String TYPE = "express";
    private static final long serialVersionUID = 4610495074511059465L;
    private static final Log LOG = LogFactory.getLog(Expression.class);
    private static final String FIELD_COMPARE = "field";
    private static final String AES_KEY = "baicheng.cbc";
    private String varName;
    private String functionName;
    @SuppressWarnings("rawtypes")
    private DataType dataType = new StringDataType(String.class);
    protected T value;
    public Expression copy() {
        Expression expression=new Expression();
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




    public Expression() {
        setType(TYPE);
    }

    protected static RegexFunction regexFunction=new RegexFunction();
    protected static LikeFunction likeFunction=new LikeFunction();
    protected static InFunction inFunction=new InFunction();
    protected static IsNotNull isNotNull=new IsNotNull();
    protected static IsNull isNull=new IsNull();
    protected static transient Map<String,ExpressionFunction> cache=new HashMap<>();
    @SuppressWarnings("unchecked")
    @Override
    public Boolean doAction(RuleContext context, Rule rule) {

        ExpressionFunction function = cache.get(functionName);
        if(function==null){
            function=context.getExpressionFunction(functionName, this, context, rule);
            cache.put(functionName,function);
        }

        if (function == null) {
            return false;
        }
        /**
         * 表达式新增fieldFlag 如：src_port=desc_port fieldFlag为true时，设置value为对应的字段值
         */
        if (fieldFlag && !initFlag) {
            synchronized (Expression.class) {
                if (fieldFlag && !initFlag) {
                    Var var = context.getVar(rule.getConfigureName(), String.valueOf(value));
                    if (var == null) {
                        LOG.error("Expression context.getVar var is null,expressName is: " + this.getConfigureName()
                            + " ,varName is: " + String.valueOf(value));
                        return false;
                    }
                    Object varObject = null;
                    varObject = var.getVarValue(context, rule);
                    if (varObject == null) {
                        LOG.error(
                            "Expression context.getVar varObject is null,expressName is: " + this.getConfigureName()
                                + " ,varName is: " + String.valueOf(value));
                        return false;
                    }
                    this.value = (T)String.valueOf(varObject).trim();
                }
            }
        }
        boolean result = false;
        try {
            if(RegexFunction.isRegex(functionName)){
                return regexFunction.doExpressionFunction(this,context,rule);
            }
            if(LikeFunction.isLikeFunciton(functionName)){
                return likeFunction.doExpressionFunction(this,context,rule);
            }
            if(InFunction.matchFunction(functionName)){
                return inFunction.doExpressionFunction(this,context,rule);
            }
            if(IsNull.matchFunction(functionName)){
                return isNull.doExpressionFunction(this,context,rule);
            }
            if(IsNotNull.matchFunction(functionName)){
                return isNotNull.doExpressionFunction(this,context,rule);
            }
            result = function.doFunction(this, context, rule);
        } catch (Exception e) {
            LOG.error(
                "Expression doAction function.doFunction error,rule is: " + rule.getConfigureName() + " ,express is:"
                    + getConfigureName(), e);
        }
        return result;
    }

    public Boolean getExpressionValue(RuleContext context, Rule rule) {

//        if(RegexFunction.isRegex(functionName)|| LikeFunction.isLikeFunciton(functionName)){
//            Boolean value=ScriptExpressionGroupsProxy.inFilterCache(getVarName(),getValue().toString(),context.getMessage(),context);
//            if(value!=null){
//                return value;
//            }
//        }

//        Boolean result = context.getExpressionValue(getConfigureName());
//        if (result != null) {
//            return result;
//        }
        try {

            long start=System.currentTimeMillis();
            Boolean isMatch=context.matchFromCache(context.getMessage(),this);
            if(isMatch!=null){
                return isMatch;
            }
            boolean result= doAction(context, rule);
            if(!RelationExpression.class.isInstance(this)){
                if((System.currentTimeMillis()-start)>10){
                    System.out.println("==============="+toJson());
                }
            }
            return result;
//            if (result != null) {
//                context.putExpressionValue(getNameSpace(), getConfigureName(), result);
//            }
        } catch (Exception e) {
            LOG.error("Expression getExpressionValue error,rule is:" + rule.getConfigureName() + " ,express is: "
                + getConfigureName(), e);
            return false;
        }
        //if(result==false){
        //    context.setNotFireExpression(this.toString());
        //}
    }

    @SuppressWarnings("unchecked")
    @Override
    public String toJson() {
        String valueStr = String.valueOf(value);
        try {
            // 适配fastjson的bug，parseArray时会对"-"进行分割
            if (ListDataType.class.isInstance(dataType) && valueStr.contains("-") && valueStr.contains(",")) {
                this.value = (T)value;
            } else {
                // value很多时候会是String类型，这样dataType.toDataJson会报错，所以先转为dataType类型,
                // list类型JsonArray.tojson会报错，不做处理（[pfile_path_rule,proc_vul_exploit_rce_rule]）
                if (!ListDataType.class.isInstance(dataType)) {
                    this.value = (T)dataType.getData(valueStr);
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
            if (StringDataType.class.isInstance(dataType)) {
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
            if (StringDataType.class.isInstance(dataType)) {
                // value经过加密 需要解密
                if (aesFlag == 1) {
                    try {
                        this.value = (T)AESUtil.aesDecrypt(valueJson, AES_KEY);
                    } catch (Exception e) {
                        this.value = (T)this.dataType.getData(valueJson);
                    }
                } else {
                    this.value = (T)this.dataType.getData(valueJson);
                }
            } else {
                this.value = (T)this.dataType.getData(valueJson);
            }
        } else {
            dataTypestr = FIELD_COMPARE;
            this.value = (T)valueJson;
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
        if (StringUtil.isEmpty(varName) || StringUtil.isEmpty(functionName) || dataType == null) {
            return false;
        }
        return true;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        return true;
    }

    public String getDataTypestr() {
        return dataTypestr;
    }

    public void setDataTypestr(String dataTypestr) {
        this.dataTypestr = dataTypestr;
        if (FIELD_COMPARE.equals(dataTypestr)) {
            fieldFlag = true;
        }

        DataType dt = MetaDataField.getDataTypeByStr(dataTypestr);
        this.dataType = dt;
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

    /**
     * 返回expressCode
     *
     * @return
     */
    @SuppressWarnings("rawtypes")
    public String toExpressionString(Map<String, Expression> name2Expression,
                                     String... expressionNamePrefixs) {
        return toString();
    }

    @Override
    public String toString() {
        String dataType = null;
        String result = "(" + varName + "," + getFunctionName() + ",";
        if (!getDataType().matchClass(String.class)) {
            dataType = getDataType().getDataTypeName();
            result += dataType + ",";
        }
        String value = getDataType().toDataJson(getValue());
        if (getDataType().matchClass(String.class) && needContants(value)) {
            value = "'" + value + "'";
        }
        result += value + ")";

        return result;
    }

    private boolean needContants(String value) {
        if (value.indexOf("(") != -1 || value.indexOf(",") != -1) {
            return true;
        }
        return false;
    }

}
