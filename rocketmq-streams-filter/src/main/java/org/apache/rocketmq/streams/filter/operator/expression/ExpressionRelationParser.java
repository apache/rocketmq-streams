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

import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;

public class ExpressionRelationParser {
    public static final String OR = "|";
    public static final String AND = "&";

    /**
     * 创建表达式
     *
     * @param namespace
     * @param str
     * @param groupList
     * @return
     */
    public static RelationExpression createRelations(String namespace, String ruleName, String str, List<RelationExpression> groupList) {
        if (str.indexOf(")") == -1) {
            return createMixRelation(namespace, ruleName, str, groupList);
        }
        int endIndex = str.indexOf(")");
        int startIndex = 0;
        for (int i = endIndex; i > 0; i--) {
            String word = str.substring(i - 1, i);
            if ("(".equals(word)) {
                startIndex = i;
                break;
            }
        }
        String expression = str.substring(startIndex, endIndex);
        RelationExpression relationExpression = createMixRelation(namespace, ruleName, expression, groupList);
        str = str.replace("(" + expression + ")", relationExpression.getConfigureName());
        return createRelations(namespace, ruleName, str, groupList);
    }

    /**
     * 混合表达关系处理
     *
     * @param namespace
     * @param str
     * @return
     */
    private static RelationExpression createMixRelation(String namespace, String ruleName, String str,
        List<RelationExpression> groupList) {
        if (str.indexOf(OR) == -1) {
            return createSignleRelation(namespace, ruleName, str, AND, groupList);
        }
        String[] values = str.split("\\" + OR);
        for (String value : values) {
            String sign = AND;
            if (value.indexOf(sign) != -1) {
                RelationExpression relationExpression = createSignleRelation(namespace, ruleName, value, sign, groupList);
                str = str.replace(value, relationExpression.getConfigureName());
            }
        }
        str = str.replace(" ", "");
        return createSignleRelation(namespace, ruleName, str, OR, groupList);
    }

    /**
     * 纯粹表达关系处理
     *
     * @param namespace
     * @param str
     * @param sign
     * @return
     */
    private static RelationExpression createSignleRelation(String namespace, String ruleName, String str, String sign, List<RelationExpression> groupList) {
        NameCreator nameCreator = NameCreatorContext.get().createOrGet(ruleName);
        String expressionName = nameCreator.createName(ruleName);
        RelationExpression relationExpression = new RelationExpression();
        relationExpression.setNameSpace(namespace);
        relationExpression.setType(Expression.TYPE);
        relationExpression.setValue(new ArrayList<String>());
        relationExpression.setConfigureName(expressionName);

        relationExpression.setRelation(OR.equals(sign) ? "or" : "and");
        String[] values = str.split("\\" + sign);
        for (String value : values) {
            relationExpression.addExpression(value);
        }
        groupList.add(relationExpression);
        return relationExpression;
    }
}
