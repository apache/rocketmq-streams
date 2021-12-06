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
package org.apache.rocketmq.streams.filter.builder;

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.contants.RuleElementType;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.operator.var.ConstantVar;
import org.apache.rocketmq.streams.filter.operator.var.ContextVar;

public class RuleElementBuilder {
    /**
     * @param namespace
     * @param name
     * @param fields    格式：fieldname;datatypename;isRequired。如isRequired==false, 最后部分可以省略，如果datatypename＝＝string，且isRequired==false，可以只写fieldname
     * @return
     */
    public static MetaData createMetaData(String namespace, String name, String... fields) {
        MetaData metaData = new MetaData();
        metaData.setNameSpace(namespace);
        metaData.setConfigureName(name);
        if (fields == null || fields.length == 0) {
            metaData.toObject(metaData.toJson());
            return metaData;
        }
        List<MetaDataField> metaDataFieldList = new ArrayList<>();
        for (String field : fields) {
            MetaDataField metaDataField = new MetaDataField();
            String sign = ":";
            if (field.indexOf(sign) == -1) {
                sign = ";";
            }
            String[] values = field.split(sign);
            String fieldName = values[0];
            metaDataField.setFieldName(fieldName);
            DataType dataType = DataTypeUtil.getDataTypeFromClass(String.class);
            boolean isRequired = false;
            if (values.length > 1) {
                String dataTypeName = values[1];
                dataType = DataTypeUtil.getDataType(dataTypeName);

            }
            metaDataField.setDataType(dataType);
            if (values.length > 2) {
                isRequired = Boolean.valueOf(values[2]);

            }
            metaDataField.setIsRequired(isRequired);
            metaDataFieldList.add(metaDataField);
        }
        metaData.getMetaDataFields().addAll(metaDataFieldList);
        metaData.toObject(metaData.toJson());
        return metaData;
    }

    public static ContextVar createContextVar(String namespace, String ruleName, String varName, String metaDataName, String fieldName) {
        ContextVar contextVar = new ContextVar();
        contextVar.setNameSpace(namespace);
        contextVar.setType(RuleElementType.VAR.getType());
        contextVar.setVarName(varName);
        contextVar.setConfigureName(MapKeyUtil.createKeyBySign("_", ruleName, varName));
        contextVar.setMetaDataName(metaDataName);
        contextVar.setFieldName(fieldName);
        return contextVar;
    }

    public static ConstantVar createConstantVar(String namespace, String ruleName, String varName, DataType dataType, String value) {
        ConstantVar constantVar = new ConstantVar();
        constantVar.setNameSpace(ruleName);
        constantVar.setDataType(dataType);
        constantVar.setVarName(varName);
        constantVar.setConfigureName(MapKeyUtil.createKeyBySign("_", ruleName, varName));
        constantVar.setValue(dataType.getData(value));
        constantVar.setType(RuleElementType.VAR.getType());
        return constantVar;
    }

    public static Expression createExpression(String ruleName, String expressionName, String varName,
                                              String functionName, DataType dataType, String value) {
        Expression expression = new Expression();
        expression.setNameSpace(ruleName);
        expression.setType(RuleElementType.EXPRESSION.getType());
        expression.setConfigureName(expressionName);
        expression.setVarName(varName);
        expression.setValue(dataType.getData(value));
        expression.setDataType(dataType);
        expression.setFunctionName(functionName);
        return expression;
    }

    public static RelationExpression createRelationExpression(String ruleName, String expressionName, String relation,
                                                              List<String> expressionNames) {
        RelationExpression relationExpression = new RelationExpression();
        relationExpression.setNameSpace(ruleName);
        relationExpression.setType(RuleElementType.EXPRESSION.getType());
        relationExpression.setConfigureName(expressionName);
        relationExpression.setRelation(relation);
        relationExpression.setValue(expressionNames);
        return relationExpression;
    }



}
