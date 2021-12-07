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
package org.apache.rocketmq.streams.dim.index;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.model.AbstractDim;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.function.expression.Equals;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.ScriptComponent;

/**
 * 执行索引的查询和构建。主要是完成表达式的解析，对于等值的表达式字段，如果有索引，根据索引查询，然后执行非等值部分的判断
 */
public class IndexExecutor {

    private String expressionStr;//表达式

    private boolean isSupport = false;//是否支持索引，比如表达式等值部分无所以，则不能走索引逻辑

    private String namespace;

    private String indexNameKey;//索引的名字，多个字段";"分隔

    private List<String> msgNames;//

    private Rule rule;//表达式会被编译成rule

    private List<String> index; //标准化后的索引name

    private Set<String> indexNames = new HashSet<>();

    private Set<String> columnNames;
    public IndexExecutor(String expressionStr, String namespace, List<String> index,Set<String> columns) {
        this.expressionStr = expressionStr;
        this.namespace = namespace;
        this.columnNames=columns;
        List<String> allIndex = new ArrayList<>();
        for (String indexName : index) {
            String[] values = indexName.split(";");
            List<String> indexList = new ArrayList<>();
            for (String value : values) {
                indexNames.add(value);
                indexList.add(value);
            }
            Collections.sort(indexList);
            String indexKey = MapKeyUtil.createKey(indexList);
            allIndex.add(indexKey);
        }
        this.index = allIndex;
        parse();
    }

    /**
     * 解析表达式，找出等值字段和非等值字段 如果有索引走索引，否则走全表扫描
     */
    protected void parse() {
        List<Expression> expressions = new ArrayList<>();
        List<RelationExpression> relationExpressions = new ArrayList<>();
        Expression expression = ExpressionBuilder.createOptimizationExpression(namespace, "tmp", expressionStr,
            expressions, relationExpressions);

        RelationExpression relationExpression = null;
        if (RelationExpression.class.isInstance(expression)) {
            relationExpression = (RelationExpression)expression;
            if (!"and".equals(relationExpression.getRelation())) {
                isSupport = false;
                return;
            }
        }

        this.isSupport=true;
        List<Expression> indexExpressions = new ArrayList<>();
        List<Expression> otherExpressions = new ArrayList<>();
        if (relationExpression != null) {
            Map<String, Expression> map = new HashMap<>();
            for (Expression tmp : expressions) {
                map.put(tmp.getConfigureName(), tmp);
            }
            for (Expression tmp : relationExpressions) {
                map.put(tmp.getConfigureName(), tmp);
            }
            List<String> expressionNames = relationExpression.getValue();
            relationExpression.setValue(new ArrayList<>());
            for (String expressionName : expressionNames) {
                Expression subExpression = map.get(expressionName);
                if (subExpression != null && !RelationExpression.class.isInstance(subExpression)&&this.indexNames.contains(subExpression.getValue())) {
                    indexExpressions.add(subExpression);
                } else {
                    otherExpressions.add(subExpression);
                    relationExpression.getValue().add(subExpression.getConfigureName());
                }
            }

        } else {
            indexExpressions.add(expression);
        }

        List<String> fieldNames = new ArrayList<>();
        Map<String, String> msgNames = new HashMap<>();

        for (Expression expre : indexExpressions) {
            if (RelationExpression.class.isInstance(expre)) {
                continue;
            }
            String indexName = expre.getValue().toString();
            if (Equals.isEqualFunction(expre.getFunctionName()) && indexNames.contains(indexName)) {

                fieldNames.add(indexName);
                msgNames.put(indexName, expre.getVarName());
            }
        }
        Collections.sort(fieldNames);
        indexNameKey = MapKeyUtil.createKey(fieldNames);
        if (!this.index.contains(indexNameKey)) {
            this.isSupport = false;
            return;
        }
        this.msgNames = createMsgNames(fieldNames, msgNames);
        if (otherExpressions.size() == 0) {
            return;
        }
        Rule rule = ExpressionBuilder.createRule("tmp","tmp",expressionStr);

        this.rule = rule;

    }

    /**
     * 创建索引字段的索引值
     *
     * @param fieldNames
     * @param msgNames
     * @return
     */
    protected List<String> createMsgNames(List<String> fieldNames, Map<String, String> msgNames) {
        List<String> msgNameList = new ArrayList<>();
        for (String fieldName : fieldNames) {
            msgNameList.add(msgNames.get(fieldName));
        }
        return msgNameList;
    }

    public boolean isSupport() {
        return isSupport;
    }

    private static IntDataType intDataType = new IntDataType();

    public List<Map<String, Object>> match(JSONObject msg, AbstractDim nameList, boolean needAll, String script) {
        List<Map<String, Object>> rows = new ArrayList<>();
        String msgValue = createValue(msg);
        List<Integer> rowIds = nameList.getNameListIndex() == null ? Collections.emptyList() :  nameList.getNameListIndex().getRowIds(indexNameKey, msgValue);;
        if (rowIds == null) {
            return null;
        }
        for (Integer rowId : rowIds) {
            Map<String, Object> oldRow = nameList.getDataCache().getRow(rowId);
            Map<String, Object> newRow=AbstractDim.isMatch(this.rule,oldRow,msg,script,this.columnNames);
            if (newRow!=null) {
                rows.add(newRow);
                if (needAll == false) {
                    return rows;
                }
            }
        }
        return rows;
    }



    /**
     * 按顺序创建msg的key
     *
     * @param msg
     * @return
     */
    private String createValue(JSONObject msg) {
        List<String> value = new ArrayList<>();
        for (String msgName : msgNames) {
            value.add(msg.getString(msgName));
        }
        return MapKeyUtil.createKey(value);
    }
}
