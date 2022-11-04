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
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 函数参数的表示，支持嵌套函数的场景，包括条件判断，嵌套函数 如果是嵌套函数，则先计算嵌套函数的值，把值作为参数结果返回，支持多层嵌套
 */
public class ScriptParameter implements IScriptParamter {

    private String scriptParameterStr;

    private String leftVarName;//如果是单指参数，存储参数。嵌套函数，当作第一个参数传递

    private String rigthVarName;//函数参数，当是嵌套函数或判断时，作为参数

    private String functionName;//函数名，可以为空，嵌套函数或判断时使用

    public ScriptParameter(String simpleParamter) {
        this.scriptParameterStr = simpleParamter;
        if (ContantsUtil.isContant(simpleParamter)||simpleParamter.startsWith("(")&&simpleParamter.endsWith(")")) {
            this.leftVarName = simpleParamter;
        } else if (simpleParamter.indexOf("==") != -1) {
            doConditonValue(simpleParamter, "==");
        } else if (simpleParamter.indexOf(">=") != -1) {
            doConditonValue(simpleParamter, ">=");
        } else if (simpleParamter.indexOf("<=") != -1) {
            doConditonValue(simpleParamter, "<=");
        } else if (simpleParamter.indexOf(">") != -1) {
            doConditonValue(simpleParamter, ">");
        } else if (simpleParamter.indexOf("<") != -1) {
            doConditonValue(simpleParamter, "<");
        } else {
            this.leftVarName = simpleParamter;
        }
    }

    private void doConditonValue(String simpleParamter, String sign) {
        String[] values = simpleParamter.split(sign);
        this.leftVarName = values[0];
        this.rigthVarName = values[1];
        this.functionName = sign;
    }

    @Override
    public Object getScriptParamter(IMessage message, FunctionContext context) {
        if (StringUtil.isNotEmpty(functionName)) {
            if (StringUtil.isNotEmpty(rigthVarName)) {
                Object value = context.executeFunction(functionName, message, leftVarName, rigthVarName);
                if (value == null) {
                    return null;
                }
                if (FunctionUtils.isNumber(value.toString()) || FunctionUtils.isBoolean(value.toString())) {
                    return value;
                } else {
                    return "'" + value + "'";
                }
            }
        } else {
            return leftVarName;
        }
        return scriptParameterStr;
    }

    @Override
    public String getScriptParameterStr() {
        return scriptParameterStr;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (StringUtil.isNotEmpty(functionName)) {
            if (StringUtil.isNotEmpty(rigthVarName)) {
                sb.append(leftVarName + functionName + rigthVarName);
            }
        } else {
            return leftVarName;
        }
        return sb.toString();
    }

    @Override
    public List<String> getDependentFields() {
        List<String> fieldNames = new ArrayList<>();
        if (StringUtil.isNotEmpty(leftVarName) && !ContantsUtil.isContant(leftVarName) && !FunctionUtils.isNumber(leftVarName) && !FunctionUtils.isBoolean(leftVarName)) {
            fieldNames.add(leftVarName);
        }
        if (StringUtil.isNotEmpty(rigthVarName) && !ContantsUtil.isContant(rigthVarName) && !FunctionUtils.isNumber(rigthVarName) && !FunctionUtils.isBoolean(rigthVarName)) {
            fieldNames.add(rigthVarName);
        }
        return fieldNames;
    }

    @Override
    public Set<String> getNewFieldNames() {
        return null;
    }

    public String getLeftVarName() {
        return leftVarName;
    }

    public String getRigthVarName() {
        return rigthVarName;
    }

    public String getFunctionName() {
        return functionName;
    }
}
