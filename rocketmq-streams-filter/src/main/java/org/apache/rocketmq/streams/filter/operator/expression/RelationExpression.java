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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RelationExpression extends Expression<List<String>> {

    private static final long serialVersionUID = -3213091464347965570L;
    private static final Logger LOGGER = LoggerFactory.getLogger(RelationExpression.class);
    private String relation = "and";                                      // and or
    // 前端处理使用
    private String expressions;                                                   // @隔开的表达式名称

    protected volatile ExpressionPerformance expressionPerformance;

    @Override
    protected void getJsonValue(JSONObject jsonObject) {
        jsonObject.put("relation", relation);
    }

    @Override
    protected void setJsonValue(JSONObject jsonObject) {
        this.relation = jsonObject.getString("relation");
    }

    protected transient Map<String, Expression> expressionMap;

    public RelationExpression() {

    }

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        expressionPerformance = new ExpressionPerformance(getValue());
        return success;
    }

    public void addExpression(String expressionName) {
        value = this.getValue();
        if (value == null) {
            value = new ArrayList<String>();
        }
        value.add(expressionName);
    }

    public Iterator<String> iterator() {
        if (value == null) {
            return null;
        }
        if (expressionPerformance != null) {
            return expressionPerformance.iterator();//expressionNames 会做优化，会给快速失效的表达式，加权中
        } else {
            return value.iterator();
        }

    }

    public boolean isOrRelation() {
        return "or".equals(relation);
    }

    @Override
    public boolean supportQuickMatch(Expression expression, RuleContext context, Rule rule) {
        Iterator<String> it = iterator();
        if (it == null) {
            return true;
        }
        while (it.hasNext()) {
            String expressionName = it.next();
            Expression e = context.getExpression(expressionName);
            if (e == null) {
                continue;
            }
            if (!e.supportQuickMatch(e, context, rule)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Boolean doMessage(IMessage message, AbstractContext context) {
        Iterator<String> it = iterator();
        /**
         * 如果表达式组的值为空，则返回false
         */
        if (!it.hasNext()) {
            return false;
        }

        boolean flag = true;
        if (StringUtil.isEmpty(relation)) {
            return false;
        }
        boolean isTrace = TraceUtil.hit(message.getHeader().getTraceId());
        if ("and".equals(this.relation)) {
            while (it.hasNext()) {
                if (flag) {
                    String expressionName = it.next();
                    Expression exp = getExpression(expressionName);
                    if (exp == null) {
                        Boolean result = message.getMessageBody().getBoolean(expressionName);
                        if (result != null) {
                            if (result == false) {
                                if (isTrace) {
                                    RuleContext.addNotFireExpressionMonitor(expressionName, context);
                                }

                                return optimizate(expressionName, false);
                            } else {
                                continue;
                            }
                        } else {
                            return false;
//                            throw new RuntimeException("expect exist expression, but not " + expressionName);
                        }

                    }

                    if (RelationExpression.class.isInstance(exp)) {
                        Boolean foreachResult = exp.doMessage(message, context);
                        if (foreachResult != null && !foreachResult) {
                            // expressions.add(exp.toString());
                            return optimizate(expressionName, false);
                        }
                    } else {
                        try {
                            flag = exp.doMessage(message, context);
                        } catch (Exception e) {
                            LOGGER.error("RelationExpression and function.doFunction error,rule is: "
                                + getConfigureName() + " ,express is: " + exp.getConfigureName(), e);
                            e.printStackTrace();
                            return false;
                        }
                        if (!flag) {

                            if (isTrace) {
                                RuleContext.addNotFireExpressionMonitor(exp, context);
                            }

                            return optimizate(expressionName, false);
                        }
                    }
                }
            }
            return true;
        } else {// or
            flag = false;

            while (it.hasNext()) {
                String expressionName = it.next();
                Expression exp = getExpression(expressionName);
                if (exp == null) {
                    Boolean result = message.getMessageBody().getBoolean(expressionName);
                    if (result != null) {
                        if (result == true) {
                            return optimizate(expressionName, true);
                        } else {
                            continue;
                        }
                    } else {
                        return false;
//                        throw new RuntimeException("expect exist expression, but not " + expressionName);
                    }
                }
                if (RelationExpression.class.isInstance(exp)) {
                    Boolean foreachResult = exp.doMessage(message, context);
                    if (foreachResult != null && foreachResult) {
                        // expressions.add(exp.toString());
                        return optimizate(expressionName, true);
                    } else {
                        //如果关系表达式未触发，则检测context中，有没有因为and失败的条件

                    }
                } else {

                    try {
                        flag = exp.doMessage(message, context);
                    } catch (Exception e) {
                        LOGGER.error(
                            "RelationExpression or function.doFunction error,rule is: " + getConfigureName()
                                + " ,express is: " + exp.getConfigureName(), e);
                    }
                    if (flag) {
                        return optimizate(expressionName, true);
                    }
                }

            }
            if (isTrace) {
                RuleContext.addNotFireExpressionMonitor(this, context);
            }

            return false;
        }

    }

    private Expression getExpression(String name) {
        return this.expressionMap.get(name);
    }

    public Boolean optimizate(String exprssionName, Boolean value) {
//        if (expressionPerformance == null) {
//            synchronized (this) {
//                if (expressionPerformance == null) {
//                    expressionPerformance = new ExpressionPerformance(getValue());
//                }
//            }
//
//        }
//        return expressionPerformance.optimizate(exprssionName, value);
        return value;
    }

    public String getRelation() {
        return relation;
    }

    public void setRelation(String relation) {
        this.relation = relation;
    }

    /**
     * 合法性验证
     *
     * @return
     */
    @Override
    public boolean volidate() {
        return true;
    }

    public String getExpressions() {
        return expressions;
    }

    public void setExpressions(String expressions) {
        if (expressions == null) {
            return;
        }
        this.expressions = expressions.trim();

        String[] exps = expressions.split("@");
        if (exps.length > 0) {
            for (String express : exps) {
                // 防止表达式组里依赖自己，造成死循环
                if (this.getConfigureName().equals(express)) {
                    continue;
                }
                this.addExpression(express.trim());
            }
        }

    }

    @Override
    public Set<String> getDependentFields(Map<String, Expression> expressionMap) {
        Set<String> set = new HashSet<>();
        if (getValue() == null) {
            return set;
        }
        for (String expressionName : value) {
            Expression expression = expressionMap.get(expressionName);
            if (expression == null) {
                if (StringUtil.isNotEmpty(expressionName) && !FunctionUtils.isBoolean(expressionName)) {
                    set.add(expressionName);
                }

            } else {
                set.addAll(expression.getDependentFields(expressionMap));
            }
        }
        return set;
    }

    /**
     * 前端展示用，不设置value值
     *
     * @param expressions
     */
    public void setExpressionsNotValue(String expressions) {
        if (expressions == null) {
            return;
        }
        this.expressions = expressions.trim();
    }
    @Override
    public String toExpressionString(Map<String, Expression> name2Expression,
        String... expressionNamePrefixs) {
        return toExpressionString(name2Expression,0,expressionNamePrefixs);
    }
    @Override
    public String toExpressionString(Map<String, Expression> name2Expression,int blackCount,
        String... expressionNamePrefixs) {
        String sign = "and".equals(relation) ? " and " : " or ";
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        sb.append("<p>");
        boolean isFirst = true;
        for (String expressionName : this.getValue()) {
            String expressKey = expressionName;
            Expression expression = null;
            if (expressionNamePrefixs != null && expressionNamePrefixs.length > 0) {
                for (String prefix : expressionNamePrefixs) {
                    expressKey = prefix + expressionName;
                    expression = name2Expression.get(expressKey);
                    if (expression != null) {
                        break;
                    }
                }
            } else {
                expression = name2Expression.get(expressKey);
            }

            if (expression == null) {
                continue;
            }
            if (isFirst) {
                isFirst = false;
            } else {
                for(int i=0;i<blackCount;i++){
                    sb.append("<br>&nbsp;&nbsp;&nbsp;");

                }
                sb.append(sign);
                sb.append("<br>");
            }
            sb.append("&nbsp;"+
                expression.toExpressionString(name2Expression,(blackCount+1), expressionNamePrefixs)+"<br>");
        }
        sb.append("<br>");
        sb.append(")");
        return sb.toString();
//        String sign = relation;
//        StringBuilder sb = new StringBuilder();
//        sb.append("(<br>");
//        boolean isFirst = true;
//        for (String expressionName : this.getValue()) {
//            String expressKey = expressionName;
//            Expression expression = null;
//            if (expressionNamePrefixs != null && expressionNamePrefixs.length > 0) {
//                for (String prefix : expressionNamePrefixs) {
//                    expressKey = prefix + expressionName;
//                    expression = name2Expression.get(expressKey);
//                    if (expression != null) {
//                        break;
//                    }
//                }
//            } else {
//                expression = name2Expression.get(expressKey);
//            }
//
//            if (isFirst) {
//                isFirst = false;
//            } else {
//                sb.append(" "+sign+" ");
//            }
//            sb.append(
//               expression==null?expressionName: expression.toExpressionString(name2Expression, expressionNamePrefixs)+ "<br> ");
//        }
//        sb.append(")");
//        return sb.toString();
    }

    public Map<String, Expression> getExpressionMap() {
        return expressionMap;
    }

    public void setExpressionMap(
        Map<String, Expression> expressionMap) {
        this.expressionMap = expressionMap;
    }
}
