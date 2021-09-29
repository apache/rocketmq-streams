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
package org.apache.rocketmq.streams.filter.operator.expression;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.optimization.CalculationResultCache;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.contants.RuleElementType;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.optimization.EqualsExpressionOptimization;
import org.apache.rocketmq.streams.filter.optimization.IExpressionOptimization;
import org.apache.rocketmq.streams.filter.optimization.LikeExpressionOptimization;
import org.apache.rocketmq.streams.filter.optimization.OptimizationExpression;
import org.apache.rocketmq.streams.filter.optimization.RegexExpressionOptimization;

/**
 * 变量名就是字段名，不需要声明meta
 */
public class SimpleExpression extends Expression {
    protected static CalculationResultCache calculationResultCache = CalculationResultCache.getInstance();

    public SimpleExpression() {
    }

    public SimpleExpression(String msgFieldName, String functionName, String value) {
        this(msgFieldName, functionName, DataTypeUtil.getDataTypeFromClass(String.class), value);
    }

    public SimpleExpression(String msgFieldName, String functionName, DataType dataType, String value) {
        setType(RuleElementType.EXPRESSION.getType());
        setFunctionName(functionName);
        setVarName(msgFieldName);
        if (dataType == null) {
            dataType = DataTypeUtil.getDataTypeFromClass(String.class);
        }
        setValue(dataType.getData(value));
        setDataType(dataType);
    }

//    @Override
//    public Boolean getExpressionValue(RuleContext context, Rule rule) {
//        IExpressionOptimization expressionOptimization = getOptimize();
//        Boolean isHitCache = null;
//        String regex = null;
//        String value = null;
//        if (expressionOptimization != null) {
//            OptimizationExpression expression = expressionOptimization.optimize(this);
//            regex = expression.getRegex();
//            String varName = expression.getVarName();
//            value = context.getMessage().getMessageBody().getString(varName);
//            isHitCache = calculationResultCache.match(regex, value);
//            if (isHitCache != null) {
//                return isHitCache;
//            }
//        }
//        boolean isMatch = super.getExpressionValue(context, rule);
//        if (isHitCache == null && expressionOptimization != null) {
//            calculationResultCache.registeRegex(regex, value, isMatch);
//        }
//        return isMatch;
//    }

    /**
     * 找到函数对应的优化
     *
     * @return
     */
    private IExpressionOptimization getOptimize() {
        for (IExpressionOptimization expressionOptimization : expressionOptimizations) {
            if (expressionOptimization.support(this)) {
                return expressionOptimization;
            }
        }
        return null;
    }

    private static List<IExpressionOptimization> expressionOptimizations = new ArrayList<>();

    static {
        expressionOptimizations.add(new EqualsExpressionOptimization());
        expressionOptimizations.add(new RegexExpressionOptimization());
        expressionOptimizations.add(new LikeExpressionOptimization());
    }

    public boolean doExecute(JSONObject msg) {
        return ExpressionBuilder.executeExecute(this, msg);
    }

    public void setMsgFieldName(String msgFieldName) {
        setVarName(msgFieldName);
    }

}
