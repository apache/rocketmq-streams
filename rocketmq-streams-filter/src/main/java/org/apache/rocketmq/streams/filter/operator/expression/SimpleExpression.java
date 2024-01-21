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
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.contants.RuleElementType;

/**
 * 变量名就是字段名，不需要声明meta
 */
public class SimpleExpression extends Expression {

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

    public boolean doExecute(JSONObject msg) {
        return ExpressionBuilder.executeExecute(this, msg);
    }

    public void setMsgFieldName(String msgFieldName) {
        setVarName(msgFieldName);
    }

}
