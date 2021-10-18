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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.monitor.TopologyFilterMonitor;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

public class RelationExpression extends Expression<List<String>> {

    private static final long serialVersionUID = -3213091464347965570L;
    private static final Log LOG = LogFactory.getLog(RelationExpression.class);
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
    public Boolean doAction(RuleContext context, Rule rule) {

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
        Message message = context.getMessage();
        boolean isTrace=TraceUtil.hit(message.getHeader().getTraceId());
        if ("and".equals(this.relation)) {
            while (it.hasNext()) {
                if (flag) {
                    String expressionName = it.next();
                    Expression exp = context.getExpression(expressionName);
                    if (exp == null) {
                        Boolean result = message.getMessageBody().getBoolean(expressionName);
                        if (result != null) {
                            if (result == false) {
                                if(isTrace){
                                    TopologyFilterMonitor piplineExecutorMonitor = new TopologyFilterMonitor();
                                    piplineExecutorMonitor.addNotFireExpression(expressionName, expressionName);
                                    context.setExpressionMonitor(piplineExecutorMonitor);
                                }

                                return optimizate(expressionName, false);
                            } else {
                                continue;
                            }
                        } else {
                            throw new RuntimeException("expect exist expression, but not " + expressionName);
                        }

                    }

                    if (RelationExpression.class.isInstance(exp)) {
                        Boolean foreachResult = exp.doAction(context, rule);
                        if (foreachResult != null && !foreachResult) {
                            // expressions.add(exp.toString());
                            return optimizate(expressionName, false);
                        }
                    } else {
                        try {
                            flag = exp.getExpressionValue(context, rule);
                        } catch (Exception e) {
                            LOG.error("RelationExpression and function.doFunction error,rule is: "
                                + rule.getConfigureName() + " ,express is: " + exp.getConfigureName(), e);
                            e.printStackTrace();
                            return false;
                        }
                        if (!flag) {
                            if(isTrace){
                                TopologyFilterMonitor piplineExecutorMonitor = new TopologyFilterMonitor();
                                piplineExecutorMonitor.addNotFireExpression(exp.toString(), exp.getDependentFields(rule.getExpressionMap()));
                                context.setExpressionMonitor(piplineExecutorMonitor);
                            }

                            return optimizate(expressionName, false);
                        }
                    }
                }
            }
            return true;
        } else {// or
            flag = false;
            TopologyFilterMonitor piplineExecutorMonitor =null;
            if(isTrace){
                piplineExecutorMonitor= new TopologyFilterMonitor();
            }

            while (it.hasNext()) {
                String expressionName = it.next();
                Expression exp = context.getExpression(expressionName);
                if (exp == null) {
                    Boolean result = message.getMessageBody().getBoolean(expressionName);
                    if (result != null) {
                        if (result == true) {
                            return optimizate(expressionName, true);
                        } else {
                            continue;
                        }
                    } else {
                        throw new RuntimeException("expect exist expression, but not " + expressionName);
                    }
                }
                if (RelationExpression.class.isInstance(exp)) {
                    Boolean foreachResult = exp.doAction(context, rule);
                    if (foreachResult != null && foreachResult) {
                        // expressions.add(exp.toString());
                        return optimizate(expressionName, true);
                    } else {
                        //如果关系表达式未触发，则检测context中，有没有因为and失败的条件

                        if(isTrace){
                            if (context.getExpressionMonitor() != null && context.getExpressionMonitor().getNotFireExpression2DependentFields().size() > 0) {
                                piplineExecutorMonitor.addNotFireExpression(context.getExpressionMonitor().getNotFireExpression2DependentFields());
                            }
                        }

                    }
                } else {

                    try {
                        flag = exp.getExpressionValue(context, rule);
                    } catch (Exception e) {
                        LOG.error(
                            "RelationExpression or function.doFunction error,rule is: " + rule.getConfigureName()
                                + " ,express is: " + exp.getConfigureName(), e);
                    }
                    if (flag) {
                        return optimizate(expressionName, true);
                    }
                }

            }
            if(isTrace){
                context.setExpressionMonitor(piplineExecutorMonitor);
            }

            return false;
        }

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
        String sign = "and".equals(relation) ? "&" : "|";
        StringBuilder sb = new StringBuilder();
        sb.append("(");
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
                sb.append(sign);
            }
            sb.append(
                expression.toExpressionString(name2Expression, expressionNamePrefixs));
        }
        sb.append(")");
        return sb.toString();
    }

}
