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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.optimization.CalculationResultCache;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

@Function

public class RegexFunction extends AbstractExpressionFunction {

    protected static CalculationResultCache calculationResultCache = CalculationResultCache.getInstance();

    private static final Log LOG = LogFactory.getLog(RegexFunction.class);
    private static final int REGEX_TIME_OUT = -1;

    public static boolean isRegex(String functionName) {
        if ("regex".equals(functionName) || "regexCaseInsensitive".equals(functionName) || "~regex".equals(functionName)) {
            return true;
        }
        return false;
    }

    public static boolean isNotRegex(String functionName) {
        if ("notRegex".equals(functionName) || "!regex".equals(functionName) || "notRegexCaseInsensitive".equals(functionName) || "~!regex".equals(functionName)) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    @Override
    @FunctionMethod("regex")
    @FunctionMethodAilas("正则匹配")
    public Boolean doExpressionFunction(Expression expression, RuleContext context, Rule rule) {
        if (!expression.volidate()) {
            return false;
        }
        Message message = context.getMessage();

        Var var = context.getVar(rule.getConfigureName(), expression.getVarName());
        if (var == null) {
            return false;
        }
        Object varObject = null;
        Object valueObject = null;
        varObject = var.getVarValue(context, rule);

        valueObject = expression.getValue();

        if (varObject == null || valueObject == null) {
            return false;
        }

        String varString = "";
        String regex = "";
        varString = String.valueOf(varObject).trim();
        regex = String.valueOf(valueObject).trim();
        Boolean isMatch = calculationResultCache.match(regex, varString);
        if (isMatch != null) {
            return isMatch;
        }
        boolean value = false;
        if (caseInsensitive()) {
            value = StringUtil.matchRegexCaseInsensitive(varString, regex);
        } else {
            value = StringUtil.matchRegex(varString, regex);
        }
        if (isMatch == null) {
            calculationResultCache.registeRegex(regex, varString, value);
        }
        return value;
    }

    @SuppressWarnings({"rawtypes"})
    protected boolean doPreProcess(RuleContext context, Expression expression, String varString) {

        try {
            String keyword = expression.getKeyword();
            if (StringUtil.isEmpty(keyword) || StringUtil.isEmpty(varString)) {
                return true;
            }
            boolean result = true;
            if (varString.toLowerCase().indexOf(keyword) == -1) {
                result = false;
            }

            return result;
        } catch (Exception e) {
            LOG.error("AbstractExpressionFunction doPreProcess error", e);
            return true;
        }

    }

    protected boolean caseInsensitive() {
        return false;
    }
}
