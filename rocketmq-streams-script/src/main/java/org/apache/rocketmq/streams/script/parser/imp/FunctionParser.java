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
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.parser.ScriptExpressionParserFactory;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.ScriptParserUtil;

/**
 * 常规函数的解析，不包括ifelse
 */
public class FunctionParser {

    public final static String LINE_SEPERATOR = System.getProperty("line.separator");

    public static final String TAB = "   ";

    private static FunctionParser functionParser = new FunctionParser();

    private FunctionParser() {

    }

    public static FunctionParser getInstance() {
        return functionParser;
    }

    public static void main(String[] args) {
        String script =
            "if(min_day_prediction>=prediction_day){prediction_day=min_day_prediction;}else{echo();};"
                + "until_day=datefirst(now,'dd');";

        FunctionParser functionParser = new FunctionParser();
        Map map = new HashMap<String, String>();
        String result = functionParser.doConditionParser(script, map, 1);
        System.out.println(result);
    }

    public List<IScriptExpression> parse(String value) {
        if (StringUtil.isEmpty(value)) {
            return new ArrayList<>();
        } else {
            return parserScript(value);
        }
    }

    protected List<IScriptExpression> parserScript(String script) {
        Map<String, String> flag2ExpressionStr = new HashMap<>();
        String expressionStr = ScriptParserUtil.doConstantReplace(script, flag2ExpressionStr, 1);
        expressionStr = doConditionParser(expressionStr, flag2ExpressionStr, flag2ExpressionStr.size() + 1);
        String[] values = expressionStr.split(";");
        List<IScriptExpression> scripts = new ArrayList<IScriptExpression>();
        for (String value : values) {
            try {
                String scriptStr = value.replace(LINE_SEPERATOR, "").trim();
                if (StringUtil.isEmpty(scriptStr)) {
                    continue;
                }
                if (flag2ExpressionStr.containsKey(scriptStr)) {
                    scriptStr = flag2ExpressionStr.get(scriptStr);
                }
                scriptStr = ScriptParserUtil.restoreConstantAndNotRestoreSpeical(scriptStr, flag2ExpressionStr);
                if (StringUtil.isNotEmpty(scriptStr)) {
                    scriptStr = scriptStr.trim();
                }
                IScriptExpression expression = ScriptExpressionParserFactory.getInstance().parse(scriptStr);
                scripts.add(expression);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("operator item parse error " + value, e);
            }
        }
        return scripts;
    }

    protected String doConditionParser(String tmp, Map<String, String> flag2ExpressionStr, int flag) {
        int firstCloseBracketesIndex = paserFunctionFirstBracketesIndex(tmp);
        if (firstCloseBracketesIndex == -1) {
            return tmp;
        }
        int functionOpenBracketesIndex = paserFunctionOpenBracketesIndex(firstCloseBracketesIndex, tmp);
        String expressionStr = tmp.substring(functionOpenBracketesIndex, firstCloseBracketesIndex);
        flag2ExpressionStr.put(ScriptParserUtil.createFlagKey(flag), expressionStr);
        tmp = tmp.replace(expressionStr, ScriptParserUtil.createFlagKey(flag));
        flag++;
        return doConditionParser(tmp, flag2ExpressionStr, flag);
    }

    private int paserFunctionOpenBracketesIndex(int firstCloseBracketesIndex, String parameterStr) {
        boolean inMode = false;
        boolean inStartMode = false;
        parameterStr = parameterStr.replace("elseif", "elseff");
        for (int i = firstCloseBracketesIndex; i > 0; i--) {
            String word = parameterStr.substring(i - 1, i);
            word = word.trim();
            if (StringUtil.isEmpty(word)) {
                continue;
            }
            if ("(".equals(word)) {
                String tmp = parameterStr.substring(0, i - 1);
                tmp = tmp.trim();
                if (tmp.endsWith("if")) {
                    return tmp.lastIndexOf("if");
                } else {
                    inStartMode = false;
                }
                continue;
            }

        }
        return -1;
    }

    private int paserFunctionFirstBracketesIndex(String parameterStr) {
        String tmp = parameterStr;
        int firstCloseBracketesIndex = tmp.indexOf("}");
        if (firstCloseBracketesIndex == -1) {
            return -1;
        }
        boolean inMode = true;//没有在if嵌套中
        int i = 0;
        for (i = firstCloseBracketesIndex + 1; i < parameterStr.length(); i++) {
            String word = parameterStr.substring(i, i + 1);
            word = word.trim();
            if (StringUtil.isEmpty(word)) {
                continue;
            }
            if (";".equals(word) && inMode) {//如果}就是；的情况
                return i;
            }
            if ("}".equals(word) && inMode) {//有else的情况
                return i - 1;
            }
            if ("}".equals(word) && !inMode) {
                inMode = true;//完成内部嵌套的情况
            } else {
                inMode = false;
            }

        }
        if (inMode) {
            return i - 1;
        }
        return -1;
    }
}
