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

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.cache.softreference.ICache;
import org.apache.rocketmq.streams.common.cache.softreference.impl.SoftReferenceCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionMethodAilas;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class InFunction extends AbstractExpressionFunction {

    /**
     * in 的字符串形成set，可以最快o（1）来提高效率
     */
    private ICache<Object, Set<String>> cache = new SoftReferenceCache();

    public static boolean matchFunction(String functionName) {
        return "in".equals(functionName) || "~in".equals(functionName);
    }

    @Override
    @FunctionMethod(value = "in", alias = "~in")
    @FunctionMethodAilas("包含")
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {

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
        String varString = String.valueOf(varObject).trim();
        Set<String> valueSet = cache.get(valueObject);
        if (valueSet != null) {
            return valueSet.contains(varString);
        }

        String valueString = "";

        valueString = String.valueOf(valueObject).trim();
        if (StringUtil.isEmpty(valueString)) {
            return false;
        }
        String[] values = valueString.split(",");
        Set<String> set = new HashSet<>();
        for (String value : values) {
            value = FunctionUtils.getConstant(value);
            set.add(value);

        }
        cache.put(valueObject, set);

        return set.contains(varString);
    }
}
