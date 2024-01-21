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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;

/**
 * 对于表达式解析，为了方便，会产生很多附加的关系，如（a,==,b)&((c,>,0)&&(d,>,1))，每一层括号都会产生一层关系。可以通过优化，降低关系个数 优化思路，是如果顶层关系是&，则子表达式也是&，则可以拉平，变成如下这种（a,==,b)&(c,>,0)&(d,>,1)
 */
public class ExpressionOptimization {
    protected Expression rootExpression;//最外层的expression
    //protected List<Expression> simpleExpressions;//不是关系的表达式
    //protected List<RelationExpression> relationExpressions;;//关系表达式
    protected Map<String, Expression> expressionMap = new HashMap<>();

    public ExpressionOptimization(Expression rootExpression, List<Expression> simpleExpressions, List<RelationExpression> relationExpressions) {
        this.rootExpression = rootExpression;
        initExpressionMap(simpleExpressions, relationExpressions);
    }

    public ExpressionOptimization(Expression rootExpression, Map<String, Expression> expressionMap) {
        this.rootExpression = rootExpression;
        this.expressionMap = expressionMap;
    }

    public static void main(String[] args) {
        String expressionName = "((a,=,c)&((d,=,e)|(f,=,d)))&(label|label2)";
        ExpressionBuilder.executeExecute("reew", expressionName, null);
    }

    /**
     * 去除不必要的关系，生成只需要的表达式和关系
     *
     * @return
     */
    public List<Expression> optimizate() {

        if (!RelationExpression.class.isInstance(rootExpression)) {
            return new ArrayList<>();
        }
        RelationExpression relationExpression = (RelationExpression) rootExpression;
        optimizate(relationExpression);
        List<Expression> expressions = new ArrayList<>();
        createExpressionList(relationExpression, expressions);
        return expressions;
    }

    /**
     * 递归，把所有关系中用到的表达式放到列表中
     *
     * @param expression
     * @param expressions
     */
    protected void createExpressionList(Expression expression, List<Expression> expressions) {
        if (expression != null) {
            expressions.add(expression);
        }
        if (RelationExpression.class.isInstance(expression) == false) {
            return;
        }
        RelationExpression relationExpression = (RelationExpression) expression;
        for (String name : relationExpression.getValue()) {
            Expression subExpression = getExpression(name);
            if (subExpression == null) {
                continue;
            }
            if (RelationExpression.class.isInstance(subExpression)) {
                createExpressionList(subExpression, expressions);
            } else {
                expressions.add(subExpression);
            }
        }
    }

    /**
     * 把expression形成name，expression的map
     */
    protected void initExpressionMap(List<Expression> simpleExpressions, List<RelationExpression> relationExpressions) {
        if (simpleExpressions != null) {
            for (Expression expression : simpleExpressions) {
                this.expressionMap.put(expression.getName(), expression);
            }
        }
        if (relationExpressions != null) {
            for (RelationExpression relationExpression : relationExpressions) {
                this.expressionMap.put(relationExpression.getName(), relationExpression);
            }
        }
    }

    /**
     * 递归，去除不必要的关系，逻辑是如果子表达式的关系和父表达的关系是一样的，则去除中间的关系
     *
     * @param parentExpression
     */
    public void optimizate(RelationExpression parentExpression) {
        String parentRelation = parentExpression.getRelation();
        List<String> names = new ArrayList<>();
        for (String expressionName : parentExpression.getValue()) {
            Expression children = getExpression(expressionName);
            if (children == null) {
                names.add(expressionName);
                continue;
            }
            if (RelationExpression.class.isInstance(children) == false) {
                names.add(expressionName);
                continue;
            }
            RelationExpression relationExpression = (RelationExpression) children;
            optimizate(relationExpression);
            if (!relationExpression.getRelation().equals(parentRelation)) {
                names.add(relationExpression.getName());
            } else {
                names.addAll(relationExpression.getValue());
            }
        }
        parentExpression.setValue(names);
    }

    private Expression getExpression(String expressionName) {
        return this.expressionMap.get(expressionName);
    }
}
