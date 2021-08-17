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
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.exception.RegexTimeoutException;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public abstract class AbstractExpressionFunction implements ExpressionFunction {

    private static final Log LOG = LogFactory.getLog(AbstractExpressionFunction.class);
    private static final Log RULEENGINE_MESSAGE_LOG = LogFactory.getLog("ruleengine_message");

    @Override
    public Boolean doFunction(Expression expression, RuleContext context, Rule rule) {
        String name = MapKeyUtil.createKey(Expression.TYPE, expression.getConfigureName());
        IMonitor monitor = null;
        if (context != null && context.getRuleMonitor() != null) {
            monitor = context.getRuleMonitor().createChildren(name);
        }
        try {
            Boolean result = doExpressionFunction(expression, context, rule);
            if (monitor != null) {
                monitor.setResult(result);
                monitor.endMonitor();
                if (monitor.isSlow()) {
                    monitor.setSampleData(context).put("expression_info", expression.toJson());
                }
            }
            return result;
        } catch (RegexTimeoutException e) {
            LOG.error("AbstractExpressionFunction RegexTimeoutException", e);
            RULEENGINE_MESSAGE_LOG.warn("AbstractExpressionFunction doFunction error", e);
            if (monitor != null) {
                monitor.occureError(e, "AbstractExpressionFunction doFunction error", e.getMessage());
                monitor.setSampleData(context).put("expression_info", expression.toJsonObject());
            }

            throw e;
        }

    }

    @SuppressWarnings("rawtypes")
    protected abstract Boolean doExpressionFunction(Expression expression, RuleContext context, Rule rule);

}
