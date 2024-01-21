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
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;

@Function
public class InMetaData extends AbstractExpressionFunction {

    @FunctionMethod("engine_in")
    @FunctionMethodAilas("in(英文逗号分隔)")
    @Override
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {
        if (!expression.volidate()) {
            return false;
        }

        Var var = expression.getVar();
        if (var == null) {
            return false;
        }
        Object varObject = null;
        Object valueObject = null;
        varObject = var.doMessage(message, context);
        valueObject = expression.getValue();

        if (varObject == null || valueObject == null) {
            return false;
        }

        String varString = "";
        String inStrings = "";
        varString = String.valueOf(varObject).trim();
        inStrings = String.valueOf(valueObject).trim();
        String[] instr = inStrings.split(",");
        for (String str : instr) {
            if (str.trim().equals(varString)) {
                return true;
            }
        }

        return false;

    }
}
