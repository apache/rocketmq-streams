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
package org.apache.rocketmq.streams.filter.function.expression;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.SimpleExpression;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.parser.imp.FunctionParser;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 可以把script脚本发布成规则引擎的表达式格式如下（"随便写会被忽略"，"operator"，"equals(fieldname,'3232')"）; 函数必须返回的是boolean值
 */

@Function
public class ScriptFunction extends AbstractExpressionFunction {
    public static final String SPLIT_SIGN = "######";//对参数进行分隔
    public static final String QUOTATION_CONVERT = "^^^^";//单引号转换

    @Override
    @FunctionMethod("operator")
    @FunctionMethodAilas("执行脚本")
    public Boolean doExpressionFunction(Expression expression, RuleContext context, Rule rule) {

        Object valueObject = expression.getValue();
        String valueString = "";

        valueString = String.valueOf(valueObject).trim();
        if (StringUtil.isEmpty(valueString)) {
            return false;
        }

        String script = createScript(valueString, context, expression);
        List<IScriptExpression> expressions = FunctionParser.getInstance().parse(script);
        if (expressions == null || expressions.size() == 0) {
            throw new RuntimeException("execute operator function error，parse function error " + valueString);
        }
        IScriptExpression scriptExpression = expressions.get(0);
        FunctionContext functionContext = new FunctionContext(context.getMessage());
        context.syncSubContext(functionContext);
        Object object = scriptExpression.executeExpression(context.getMessage(), functionContext);
        if (object == null) {
            throw new RuntimeException("execute scriptFunction error, expect return boolean value ,real is null");
        }
        if (Boolean.class.isInstance(object)) {
            return (Boolean)object;
        }
        throw new RuntimeException("execute scriptFunction error, expect return boolean value ,real is " + object.getClass().getName());
    }

    /**
     * 解析函数中的参数，如果参数是表达式，先进行表达式解析，得到参数值
     *
     * @param valueString
     * @param context
     * @param expression
     * @return
     */
    private String createScript(String valueString, RuleContext context, Expression expression) {
        valueString = FunctionUtils.getConstant(valueString);
        String functionStr = valueString;

        if (valueString.endsWith(";")) {
            functionStr = functionStr.substring(0, functionStr.length() - 1);
        }
        int index = functionStr.indexOf("(");
        String functionName = functionStr.substring(0, index);
        functionStr = functionStr.substring(index + 1, valueString.length() - 1);
        String[] values = functionStr.split(SPLIT_SIGN);
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(functionName + "(");
        boolean isFirst = true;
        for (String value : values) {
            if (isFirst) {
                isFirst = false;
            } else {
                stringBuilder.append(",");
            }
            if (isExpression(value)) {
                boolean paramter = ExpressionBuilder.executeExecute(expression.getNameSpace(), value, context.getMessage().getMessageBody());
                stringBuilder.append(paramter);
            } else {
                stringBuilder.append(value);
            }
        }
        stringBuilder.append(");");
        return stringBuilder.toString();
    }

    protected boolean isExpression(String paramter) {
        if (StringUtil.isEmpty(paramter)) {
            return false;
        }
        paramter = paramter.trim();
        if (paramter.startsWith("(") && paramter.endsWith(")")) {
            return true;
        }
        return false;
    }

    public static void main(String[] args) {
        ScriptFunction scriptFunction = new ScriptFunction();
        Expression expression = new SimpleExpression("inner_message", "operator", "'~((___lower_proc_path_1,regex,\'"
            + "(rdesktop|filebeat|\\/tmp\\/go-build|headless_shell|dongxingrpc|流量|scheduler|check|gpg-agent"
            + "|/fireball|magneticod|portainer|kube|/vfs|jingling|/netstat|busybox|/tomcat|server|ping|bitcoind"
            + "|/desktop|docker|aliprobe|safe|update|service|query|svchost\\"
            + ".exe|mozilla|firefox|ss-server|parity|aria2c|daemon|software|/perl|/redis|/gnome|/bin/ssh|msinfo\\"
            + ".exe|chrome\\.exe|/bin/ping|/ali|/usr/local/cloudmonitor/|sock|w3wp\\"
            + ".exe|/usr/local/aegis|/phantomjs|filenet|tunnel|probe)\'))'");

        Message message = new Message(new JSONObject());
        Rule rule = new Rule();
        rule.setNameSpace("3r3");
        scriptFunction.doExpressionFunction(expression, new RuleContext(new JSONObject(), rule), rule);
    }
}
