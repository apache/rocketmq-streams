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

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.exception.RegexTimeoutException;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public abstract class AbstractExpressionFunction implements ExpressionFunction {

    @Override
    public Boolean doFunction(IMessage message, AbstractContext context, Expression expression) {
        try {
            Boolean result = doExpressionFunction(message, context, expression);
            return result;
        } catch (RegexTimeoutException e) {
            e.printStackTrace();
            throw e;
        }

    }

    @SuppressWarnings("rawtypes")
    protected abstract Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression);

}
