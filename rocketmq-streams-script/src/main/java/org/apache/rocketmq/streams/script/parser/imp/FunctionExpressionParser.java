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
package org.apache.rocketmq.streams.script.parser.imp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.ScriptExpression;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.parser.IScriptExpressionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.apache.rocketmq.streams.script.utils.ScriptParserUtil;

/**
 * 表达式的解析
 */
public class FunctionExpressionParser implements IScriptExpressionParser {

    @Override
    public IScriptExpression parse(String scriptExpressionStr) {
        Map<String, String> flag2ExpressionStr = new HashMap<>();
        String expressionStr = null;
        if (scriptExpressionStr.startsWith("if('(") && (scriptExpressionStr.endsWith(")')") || scriptExpressionStr.endsWith(")');"))) {
            int startIndex = 4;
            int endIndex = scriptExpressionStr.length() - 2;
            if (scriptExpressionStr.endsWith(")');")) {
                endIndex--;
            }
            String contstant = scriptExpressionStr.substring(startIndex, endIndex);
            String flagStr = ContantsUtil.createFlagKey(1);
            flag2ExpressionStr.put(flagStr, contstant);
            expressionStr = scriptExpressionStr.replace("'" + contstant + "'", flagStr);
        } else {
            expressionStr = ScriptParserUtil.doConstantReplace(scriptExpressionStr, flag2ExpressionStr, 1);
        }

        ScriptExpression scriptExpression = new ScriptExpression();
        scriptExpression.setExpressionStr(scriptExpressionStr);
        int equalsSign = expressionStr.indexOf("=");
        String newFieldName = null;
        if (equalsSign == -1) {
            parseExpression(scriptExpression, expressionStr, flag2ExpressionStr);
            return scriptExpression;
        }
        int bracketsSign = expressionStr.indexOf("(");
        if (bracketsSign == -1) {
            List<IScriptParamter> paramters = new ArrayList<>();
            String[] values = expressionStr.split("=");
            newFieldName = values[0];
            scriptExpression.setNewFieldName(newFieldName);
            String paraStr = values[1];
            paraStr = ScriptParserUtil.restore(paraStr, flag2ExpressionStr);
            paramters.add(new ScriptParameter(paraStr));
            scriptExpression.setParameters(paramters);
            return scriptExpression;
        }
        if (equalsSign < bracketsSign) {
            String[] values = expressionStr.split("=");
            newFieldName = values[0];
            expressionStr = expressionStr.substring(newFieldName.length() + 1);
            parseExpression(scriptExpression, expressionStr, flag2ExpressionStr);
        } else {

            parseExpression(scriptExpression, expressionStr, flag2ExpressionStr);
        }
        scriptExpression.setNewFieldName(newFieldName);
        return scriptExpression;
    }

    @Override
    public boolean support(String itemStr) {
        return true;
    }

    protected void parseExpression(ScriptExpression scriptExpression, String expressionStr,
                                   Map<String, String> flag2ExpressionStr) {
        if (expressionStr == null) {
            return;
        }
        if (flag2ExpressionStr.containsKey(expressionStr)) {
            expressionStr = flag2ExpressionStr.get(expressionStr);
        }
        List<IScriptParamter> parameters = new ArrayList<>();
        //expressionStr=ScriptParserUtil.restore(expressionStr,flag2ExpressionStr);
        if (isFieldParamter(expressionStr)) {

            final String fieldName = expressionStr;
            parameters.add(new IScriptParamter() {

                @Override
                public List<String> getDependentFields() {
                    List<String> fieldNames = new ArrayList<>();
                    if (fieldName != null) {
                        fieldNames.add(fieldName);
                    }
                    return fieldNames;
                }

                @Override
                public Set<String> getNewFieldNames() {
                    return null;
                }

                @Override
                public Object getScriptParamter(IMessage message, FunctionContext context) {
                    Object value = FunctionUtils.getValue(message, context, fieldName);
                    context.putValue(fieldName, value);
                    return value;
                }

                @Override
                public String getScriptParameterStr() {
                    return fieldName;
                }

            });
        } else if (isConstant(expressionStr)) {
            final String value = expressionStr;
            parameters.add(new IScriptParamter() {

                @Override
                public List<String> getDependentFields() {
                    List<String> fieldNames = new ArrayList<>();
                    return fieldNames;
                }

                @Override
                public Set<String> getNewFieldNames() {
                    return null;
                }

                @Override
                public Object getScriptParamter(IMessage message, FunctionContext context) {
                    return FunctionUtils.getValueString(message, context, value);

                }

                @Override
                public String getScriptParameterStr() {
                    return value;
                }

            });
        } else {
            String[] values = expressionStr.split("\\(");
            String functionName = values[0];
            expressionStr = expressionStr.replace(functionName + "(", "");
            int index = expressionStr.lastIndexOf(")");
            String parameterStr = expressionStr.substring(0, index);
            parameterStr = parameterStr.trim();
            //如果参数是规则引擎的表达式，不解析，当作常量，交给具体函数处理
            if (isExpression(parameterStr)) {
                final String value = ContantsUtil.restoreConstantAndNotRestoreSpeical(parameterStr, flag2ExpressionStr);
                parameters.add(new IScriptParamter() {

                    @Override
                    public Object getScriptParamter(IMessage message, FunctionContext context) {
                        return "'" + value + "'";

                    }

                    @Override
                    public List<String> getDependentFields() {
                        List<String> fieldNames = new ArrayList<>();
                        fieldNames.add("'" + value + "'");
                        return fieldNames;
                    }

                    @Override
                    public Set<String> getNewFieldNames() {
                        return null;
                    }

                    @Override
                    public String getScriptParameterStr() {
                        return value;
                    }

                });
            } else {
                parameters = parseParameter(parameterStr, flag2ExpressionStr);
            }
            scriptExpression.setFunctionName(functionName.trim());
            scriptExpression.setParameters(parameters);
        }
    }

    /**
     * 如果参数是函数，不解析，当作常量，交给具体函数处理.判读依据是括号开头和括号结尾
     *
     * @param parameterStr
     * @return
     */
    protected boolean isExpression(String parameterStr) {
        if (parameterStr == null) {
            return false;
        }
        if (parameterStr.startsWith("(") && parameterStr.endsWith(")")) {
            return true;
        }
        return false;
    }

    private boolean isConstant(String expressionStr) {
        if (expressionStr.startsWith("'") && expressionStr.endsWith("'")) {
            return true;
        }
        if (expressionStr.startsWith("\"") && expressionStr.endsWith("\"")) {
            return true;
        }
        return false;
    }

    private boolean isFieldParamter(String expressionStr) {
        if (isConstant(expressionStr)) {
            return false;
        }
        if (expressionStr.indexOf("=") != -1) {
            return false;
        }
        if (expressionStr.indexOf("(") != -1) {
            return false;
        }
        return true;
    }

    /**
     * 解析正在表达式extraField(fieldName) ，支持嵌套函数  c(2),1,2,b(3,4),d(2)
     *
     * @param parameterStr
     */
    protected List<IScriptParamter> parseParameter(String parameterStr, Map<String, String> flag2ExpressionStr) {
        if (StringUtil.isEmpty(parameterStr)) {
            return new ArrayList();
        }
        parameterStr = parameterStr.trim();
        if (StringUtil.isEmpty(parameterStr)) {
            return null;
        }
        List<IScriptParamter> parameters = new ArrayList<IScriptParamter>();
        if (!hasExpressionParameter(parameterStr)) {

            return parserSimpleParameter(parameterStr, flag2ExpressionStr);
        }
        Map<String, IScriptParamter> parameterMap = new HashMap<>();
        int flag = 0;
        int firstCloseBracketesIndex = parameterStr.indexOf(")");
        int functionOpenBracketesIndex = paserFunctionOpenBracketesIndex(firstCloseBracketesIndex, parameterStr);
        String functionName = paserFunctionName(parameterStr, functionOpenBracketesIndex);
        String expressionStr =
            parameterStr.substring(functionOpenBracketesIndex - functionName.length(), firstCloseBracketesIndex + 1);
        String tmp = ScriptParserUtil.restore(expressionStr, flag2ExpressionStr);
        IScriptParamter scriptParameter = this.parse(tmp.trim());
        parameterMap.put(ScriptParserUtil.createConsKey(flag), scriptParameter);
        parameterStr = parameterStr.replace(expressionStr, ScriptParserUtil.createConsKey(flag));
        parameters = parseParameter(parameterStr, flag2ExpressionStr);
        rebackExpressionParameters(parameterMap, parameters);
        return parameters;
    }

    private List<IScriptParamter> parserSimpleParameter(String parameterStr, Map<String, String> flag2ExpressionStr) {
        List<IScriptParamter> paramters = new ArrayList<>();
        String[] values = parameterStr.split(",");
        for (String value : values) {
            value = ScriptParserUtil.restore(value, flag2ExpressionStr);
            paramters.add(new ScriptParameter(value.trim()));
        }
        return paramters;
    }

    private boolean hasExpressionParameter(String parameterStr) {
        if (parameterStr.indexOf("(") == -1) {
            return false;
        }
        if (parameterStr.startsWith("\"") && parameterStr.startsWith("\"")) {
            return false;
        }
        if (parameterStr.startsWith("'") && parameterStr.startsWith("'")) {
            return false;
        }

        if (parameterStr.indexOf("\"") == -1 && parameterStr.indexOf("'") == -1) {
            return true;
        }
        String tmp = parameterStr;
        boolean openString = false;
        for (int i = 0; i < tmp.length() - 1; i++) {
            String word = tmp.substring(i, i + 1);
            if ("\"".equals(word)) {
                openString = !openString;
            }
            if ("(".equals(word) && !openString) {
                return true;
            }
        }
        return false;
    }

    private void rebackExpressionParameters(Map<String, IScriptParamter> parameterMap,
                                            List<IScriptParamter> parameters) {
        for (int i = 0; i < parameters.size(); i++) {
            IScriptParamter scriptParameter = parameters.get(i);
            if (IScriptExpression.class.isInstance(scriptParameter)) {
                IScriptExpression expression = (IScriptExpression)scriptParameter;
                List<IScriptParamter> parameterList = expression.getScriptParamters();
                rebackExpressionParameters(parameterMap, parameterList);

            } else {
                String simple = scriptParameter.getScriptParameterStr();
                IScriptParamter newParameter = parameterMap.get(simple);
                if (newParameter != null) {
                    parameters.set(i, newParameter);
                }
            }
        }
    }

    private String paserFunctionName(String parameterStr, int functionOpenBracketesIndex) {
        for (int i = functionOpenBracketesIndex - 1; i > 0; i--) {
            String word = parameterStr.substring(i - 1, i);
            if ("(".equals(word) || ",".equals(word)) {
                return parameterStr.substring(i, functionOpenBracketesIndex);
            }
        }
        return parameterStr.substring(0, functionOpenBracketesIndex);
    }

    private int paserFunctionOpenBracketesIndex(int firstCloseBracketesIndex, String parameterStr) {
        for (int i = firstCloseBracketesIndex; i > 0; i--) {
            String word = parameterStr.substring(i - 1, i);
            if ("(".equals(word)) {
                return i - 1;
            }
        }
        return -1;
    }

}
