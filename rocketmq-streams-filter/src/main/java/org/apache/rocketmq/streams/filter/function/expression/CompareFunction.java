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
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public abstract class CompareFunction extends AbstractExpressionFunction {
    public static String VAR_PREFIX="##*^%$#@!*";//标识一个表达式的值式一个变量，是一个特殊处理。这个标识会放到值的前面。如变量式uuid，值会变成&&&&##$$%^*uuid
    private static final Log LOG = LogFactory.getLog(CompareFunction.class);

    @Override
    public Boolean doExpressionFunction(IMessage message, AbstractContext context, Expression expression) {
        if (!expression.volidate()) {
            return false;
        }
        Object varValue = null;
        String varName = expression.getVarName();
        Var var =expression.getVar();
        varValue = var.doMessage(message, context);

        /**
         * 两个数字比较的情况
         */
        if ((FunctionUtils.isNumber(varName) || FunctionUtils.isConstant(varName)) && varValue == null) {
            varValue = varName;
        }

        if (varValue == null || expression.getValue() == null) {
            return false;
        }
        if(StringUtil.isEmpty(varValue.toString())||StringUtil.isEmpty(expression.getValue().toString())){
            return false;
        }
        Object basicVarValue = expression.getDataType().getData(varValue.toString());
        Object basicValue = expression.getDataType().getData(expression.getValue().toString());
        if (varValue == null || expression.getValue() == null) {
            return false;
        }
        boolean match = false;
        if (basicValue == null || basicVarValue == null) {
            return false;
        }
        DataType dataType=expression.getDataType();
        if(String.class.isInstance(basicValue)&&basicValue.toString().startsWith(VAR_PREFIX)){
            String valueVarName=basicValue.toString().replace(VAR_PREFIX,"");
            basicValue=message.getMessageBody().get(valueVarName);
            dataType= DataTypeUtil.getDataTypeFromClass(varValue.getClass());
            basicVarValue=dataType.getData(varValue.toString());
        }

        Class varClass = basicVarValue == null ? dataType.getDataClass() : basicVarValue.getClass();
        Class valueClass = basicValue == null ? dataType.getDataClass() : basicValue.getClass();
        try {
            match = (Boolean) ReflectUtil.invoke(this, "compare",
                new Class[] {varClass, valueClass},
                new Object[] {basicVarValue, basicValue});
        } catch (Exception e) {
            LOG.error("CompareFunction doFunction ReflectUtil.invoke error: ", e);
        }

        return match;
    }
}
