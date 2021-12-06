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
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

@Function

public class NotContainsCaseInsensitiveFunction extends AbstractExpressionFunction {

    private static final Log LOG = LogFactory.getLog(NotContainsCaseInsensitiveFunction.class);

    @Override
    @FunctionMethod(value = "notContainsCaseInsensitiveFunction", alias = "~notContains")
    @FunctionMethodAilas("不包含(忽略大小写)")
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {

        try {
            if (!expression.volidate()) {
                return false;
            }

            Var var = expression.getVar();
            if (var == null) {
                return false;
            }
            Object varObject = null;
            Object valueObject = null;
            varObject = var.doMessage(message,context);
            valueObject = expression.getValue();
            if (varObject == null) {
                return true;
            }
            if (valueObject == null) {
                return false;
            }

            String varString = "";
            String valueString = "";
            varString = String.valueOf(varObject).trim();
            valueString = String.valueOf(valueObject).trim();
            if (varString.toLowerCase().contains(valueString.toLowerCase())) {
                return false;
            } else {
                return true;
            }
        } catch (Exception e) {
            LOG.error("ContainsCaseInsensitiveFunction error: rule name is: " + expression.getConfigureName(), e);
            return false;
        }

    }
}
