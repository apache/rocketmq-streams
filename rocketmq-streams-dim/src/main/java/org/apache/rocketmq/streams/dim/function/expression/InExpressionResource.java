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
package org.apache.rocketmq.streams.dim.function.expression;

import org.apache.rocketmq.streams.dim.model.DBDim;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.function.expression.AbstractExpressionFunction;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

import java.util.Map;

@Function
public class InExpressionResource extends AbstractExpressionFunction {

    /**
     * value格式 :resourceName.fieldName。如果只有单列，可以省略.fieldname
     *
     * @param expression
     * @param context
     * @param rule
     * @return
     */
    @FunctionMethod(value = "in_expression_resouce", alias = "in_resouce")
    @FunctionMethodAilas("in_expression_resouce(resourceName->(varName,functionName,value)&((varName,functionName,"
        + "value)|(varName,functionName,value)))")
    @Override
    public Boolean doExpressionFunction(Expression expression, RuleContext context, Rule rule) {
        return match(expression, context, rule, false);
    }

    protected Boolean match(Expression expression, RuleContext context, Rule rule, boolean supportRegex) {
        Object value = expression.getValue();
        if (value == null) {
            return null;
        }
        String valueStr = String.valueOf(value);
        String[] valueArray = valueStr.split("->");
        String dataResourceNamespace = rule.getNameSpace();
        String dataResourceName = null;
        String expressionStr = null;
        if (valueArray.length == 2) {
            dataResourceName = valueArray[0];
            expressionStr = valueArray[1];
        }
        if (valueArray.length > 2) {
            dataResourceNamespace = valueArray[0];
            dataResourceName = valueArray[1];
            expressionStr = valueArray[2];
        }

        DBDim dataResource =
            (DBDim)context.getConfigurableService().queryConfigurableByIdent(DBDim.TYPE, dataResourceName);
        if (dataResource == null) {
            return null;
        }
        Map<String, Object> row = dataResource.matchExpression(expressionStr, context.getMessage().getMessageBody());
        if (row != null && row.size() > 0) {
            return true;
        }
        return false;
    }
}
