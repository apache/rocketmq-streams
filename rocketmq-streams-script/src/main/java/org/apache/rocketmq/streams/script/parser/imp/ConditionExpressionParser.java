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
import org.apache.rocketmq.streams.script.parser.IScriptExpressionParser;
import org.apache.rocketmq.streams.script.parser.ScriptExpressionParserFactory;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.ScriptParserUtil;

/**
 * 主要做if(){}elseif{}else{} 的解析
 */
public class ConditionExpressionParser implements IScriptExpressionParser {

    public final static String LINE_SEPERATOR = System.getProperty("line.separator");

    private List<String> keywords = new ArrayList<>();

    private static final String ELSEIF = "elseif";

    public ConditionExpressionParser(String... conditonKeywords) {
        if (conditonKeywords == null) {
            return;
        }
        for (String conditionKeyword : conditonKeywords) {
            keywords.add(conditionKeyword);
        }
    }

    @Override
    public GroupScriptExpression parse(String itemStr) {
        GroupScriptExpression groupScriptExpression = new GroupScriptExpression();
        Map<String, String> flag2ExpressionStr = new HashMap<>();
        parse(itemStr, groupScriptExpression, groupScriptExpression, flag2ExpressionStr);
        return groupScriptExpression;
    }

    protected GroupScriptExpression parse(String itemStr, GroupScriptExpression current, GroupScriptExpression root,
                                          Map<String, String> flag2ExpressionStr) {
        int startIndex = 0;

        List<IScriptExpression> list = new ArrayList<>();

        itemStr = ScriptParserUtil.doConstantReplace(itemStr, flag2ExpressionStr, 1);

        int endIndex = itemStr.indexOf("{");
        String splitSign = "";
        int elseEndIndex = itemStr.indexOf(ELSEIF);
        if (elseEndIndex == -1) {
            elseEndIndex = itemStr.indexOf("else");
            splitSign = "else";
        }

        String expressionStr = null;
        if (endIndex == -1 && elseEndIndex == -1) {
            expressionStr = itemStr;
            List<IScriptExpression> expressions = parseScriptExpression(expressionStr, flag2ExpressionStr);
            current.setIfExpresssion(expressions.get(0));
        } else if (endIndex < elseEndIndex || elseEndIndex == -1) {
            expressionStr = itemStr.substring(startIndex, endIndex);
            List<IScriptExpression> expressions = parseScriptExpression(expressionStr, flag2ExpressionStr);
            current.setIfExpresssion(expressions.get(0));
            expressionStr = itemStr.substring(endIndex);
            int thenIndex = expressionStr.indexOf("}");
            String then = expressionStr.substring(1, thenIndex);
            expressions = parseScriptExpression(then, flag2ExpressionStr);
            current.setThenExpresssions(expressions);
            expressionStr = expressionStr.substring(thenIndex + 1);
            if (expressionStr.trim().startsWith(ELSEIF)) {
                String tmp = expressionStr.replaceFirst(ELSEIF, "if");
                GroupScriptExpression expression = new GroupScriptExpression();
                parse(tmp, expression, root, flag2ExpressionStr);
                if (expression.getElseExpressions() != null && expression.getElseExpressions().size() > 0) {
                    root.setElseExpressions(expression.getElseExpressions());
                    expression.setElseExpressions(null);
                }
                root.getElseIfExpressions().add(expression);
                return expression;
            }
            startIndex = expressionStr.trim().indexOf("else");
            if (startIndex > -1) {
                int elseIndex = expressionStr.indexOf("}");
                expressionStr = expressionStr.substring(startIndex + splitSign.length() + 1, elseIndex);
                expressions = parseScriptExpression(expressionStr, flag2ExpressionStr);
                current.setElseExpressions(expressions);
            }
        } else {
            expressionStr = itemStr.substring(startIndex, elseEndIndex);
            List<IScriptExpression> expressions = parseScriptExpression(expressionStr, flag2ExpressionStr);
            current.setIfExpresssion(expressions.get(0));
            expressionStr = itemStr.substring(elseEndIndex);
            if (expressionStr.trim().startsWith(ELSEIF)) {
                String tmp = expressionStr.replaceFirst(ELSEIF, "if");
                GroupScriptExpression expression = new GroupScriptExpression();
                parse(tmp, expression, root, flag2ExpressionStr);
                if (expression.getElseExpressions() != null && expression.getElseExpressions().size() > 0) {
                    root.setElseExpressions(expression.getElseExpressions());
                    expression.setElseExpressions(null);
                }
                root.getElseIfExpressions().add(expression);
                return expression;
            }
            startIndex = getElseIndex(expressionStr);
            if (startIndex > -1) {
                int elseIndex = expressionStr.indexOf("}");
                expressionStr = expressionStr.substring(startIndex, elseIndex);
                expressions = parseScriptExpression(expressionStr, flag2ExpressionStr);
                current.setElseExpressions(expressions);
            }
        }

        return current;
    }

    protected int getElseIndex(String expressionStr) {
        int startIndex = expressionStr.trim().indexOf("else");
        boolean success = true;
        for (int i = 4; i < expressionStr.length(); i++) {
            String word = expressionStr.substring(i, i + 1);
            if (word.trim().equals("")) {
                continue;
            }

            if (word.trim().equals("{") && success) {
                return i + 1;
            } else {
                startIndex = expressionStr.trim().indexOf("else", i);
                i = startIndex;
            }
        }
        return -1;
    }

    private List<IScriptExpression> parseScriptExpression(String expressionStr,
                                                          Map<String, String> flag2ExpressionStr) {
        expressionStr = expressionStr.trim();
        if (expressionStr.startsWith("if((")) {
            expressionStr = expressionStr.replace("if((", "if('(");
            expressionStr = expressionStr.substring(0, expressionStr.length() - 1);
            expressionStr = expressionStr + "')";
        }
        String[] values = expressionStr.split(";");
        List<IScriptExpression> expressions = new ArrayList<>();
        for (String value : values) {
            String expStr = value.replace(LINE_SEPERATOR, "").trim();
            expStr = ScriptParserUtil.restore(expStr, flag2ExpressionStr);
            if (StringUtil.isNotEmpty(expStr)) {
                expStr = expStr.trim();
            }
            IScriptExpression expression = ScriptExpressionParserFactory.getInstance().parse(expStr.trim());
            expressions.add(expression);
        }
        return expressions;
    }

    @Override
    public boolean support(String itemStr) {
        if (keywords == null || keywords.size() == 0) {
            return false;
        }
        if (itemStr.indexOf("if") == -1) {
            return false;
        }
        if (itemStr.indexOf("}") == -1) {
            return false;
        }
        int ifIndex = itemStr.indexOf("if");
        int bracketesIndex = itemStr.indexOf("(");
        String tmp = itemStr.substring(ifIndex, bracketesIndex);
        tmp = tmp.trim();
        if ("if".equals(tmp)) {
            return true;
        }
        return true;
    }

    public static void main(String[] args) {
        String scriptValue
            = "if((___lower_cmdline_10026,like,'%syncappvpublishingserver%\"%'))"
            + "{___case_10001='win_exec_payload_syncappvpublishingserver';}elseif((___lower_cmdline_10027,like,"
            + "'%runscripthelper%surfacecheck%')){___case_10001='win_exec_payload_runscripthelper';}elseif("
            + "(___lower_cmdline_10028,like,'%winword%/l%.dll')){___case_10001='win_exec_payload_winword';}elseif("
            + "(___lower_cmdline_10029,like,'%indefaultinstall%inf%'))"
            + "{___case_10001='win_exec_payload_indefaultinstall';}elseif(___regexp_10013)"
            + "{___case_10001='win_exec_payload_mavinject32';}elseif((___lower_cmdline_10031,like,'%fsi.exe%c:%'))"
            + "{___case_10001='win_exec_payload_fsi';}elseif(___regexp_10014){___case_10001='win_exec_payload_te';"
            + "}elseif(___regexp_10015){___case_10001='win_exec_payload_atbroker';}elseif(___regexp_10016)"
            + "{___case_10001='win_exec_payload_bginfo';}elseif(___regexp_10017)"
            + "{___case_10001='win_exec_payload_dnx';}elseif((___regexp_10018&___regexp_10019))"
            + "{___case_10001='win_exec_payload_rcsi';}elseif((___regexp_10020&___regexp_10021))"
            + "{___case_10001='win_exec_payload_msxsl';}elseif(___regexp_10022)"
            + "{___case_10001='win_exec_payload_ieexec';}elseif(___regexp_10023)"
            + "{___case_10001='win_exec_payload_tracker';}elseif((___lower_cmdline_10040,like,'%pubprn"
            + ".vbs%operator%http%')){___case_10001='win_exec_payload_pubprn';}elseif((___lower_cmdline_10041,like,"
            + "'%cscript%/b%slmgr.vbs%')){___case_10001='win_exec_payload_slmgr_vbs';}elseif((___lower_cmdline_10042,"
            + "like,'%cscript%/b%winrm%')){___case_10001='win_exec_payload_winrm';}elseif((___lower_cmdline_10043,"
            + "like,'%cl_invocation.ps1%syncinvoke%')){___case_10001='win_exec_payload_cl_invocation';"
            + "}else{___case_10001=null_10001;};";
        String value = scriptValue;
        value = value.replace("’", "'");
        value = value.replace("‘", "'");
        value = value.replace("’", "'");
        List<IScriptExpression> scriptExpressions = FunctionParser.getInstance().parse(value);
    }

}
