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
package org.apache.rocketmq.streams.filter.optimization;

import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.function.expression.RegexFunction;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class RegexExpressionOptimization implements IExpressionOptimization {
    @Override
    public boolean support(Expression expression) {
        return RegexFunction.isRegex(expression.getFunctionName()) && DataTypeUtil.isString(expression.getDataType()) && expression.getValue() != null;
    }

    @Override
    public OptimizationExpression optimize(Expression expression) {
        Object value = expression.getValue();
        String regex = null;
        if (value != null) {
            regex = (String)value;
        }
        OptimizationExpression optimizationExpression = new OptimizationExpression(expression, regex);
        return optimizationExpression;
    }
}
