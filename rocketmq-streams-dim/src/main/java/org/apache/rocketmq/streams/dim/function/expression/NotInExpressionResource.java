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

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

@Function
public class NotInExpressionResource extends InExpressionResource {

    /**
     * value格式 :resourceName.fieldName。如果只有单列，可以省略.fieldname
     *
     * @param expression
     * @param context
     * @return
     */
    @FunctionMethod("not_in_expression_resouce")
    @FunctionMethodAilas("not_in_expression_resouce(resourceName->(varName,functionName,value)&((varName,"
        + "functionName,value)|(varName,functionName,value)))")
    @Override
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {
        return !match(message,context,expression, false);
    }

}
