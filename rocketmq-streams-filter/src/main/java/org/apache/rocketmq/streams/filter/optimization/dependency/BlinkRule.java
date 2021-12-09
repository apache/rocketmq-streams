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
package org.apache.rocketmq.streams.filter.optimization.dependency;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class BlinkRule {
    /**
     * rule id
     */
    protected int ruleId;
    /**
     * key: varName
     * value: expression str list
     */
    protected Map<String, List<Expression>> var2ExpressionList=new HashMap<>();

    public BlinkRule(JSONObject ruleJson, int ruleId) {
        this.ruleId=ruleId;
        Iterator iterator = ruleJson.keySet().iterator();
        while (iterator.hasNext()) {
            String varName = (String) iterator.next();
            String ruleValue = ruleJson.getString(varName);
            Expression expression = null;
            if (ruleValue.startsWith("$") && ruleValue.endsWith("$")) {
                String regex = ruleValue.substring(1, ruleValue.length() - 1);
                regex=regex.replace("'","\'");
                expression = createExpression(varName,"regex",regex);
                addExpression(expression);
            } else if (ruleValue.startsWith("[") && ruleValue.endsWith("]")) {
                long start;
                long end;
                try {
                    start = Long.valueOf(ruleValue.substring(1, ruleValue.indexOf(",")));
                    end = Long.valueOf(ruleValue.substring(ruleValue.indexOf(",") + 1, ruleValue.length() - 1));
                } catch (Exception var11) {
                    start = 1L;
                    end = 0L;
                }
                Expression  expressionLeft = createExpression(varName,">=",start);
                Expression expressionRigth = createExpression(varName,",<=,",end);
                addExpression(expressionLeft);
                addExpression(expressionRigth);

            } else {
                expression = createExpression(varName,"=",ruleValue);
                addExpression(expression);
            }

        }
    }

    protected void addExpression(Expression expression) {
        String varName=expression.getVarName();
        List<Expression> expressions = var2ExpressionList.get(varName);
        if (expressions == null) {
            expressions = new ArrayList<>();
            var2ExpressionList.put(varName, expressions);
        }
        expressions.add(expression);
    }

    public Expression createExpression(String  varName,String functionName,Object value){
        Expression expression = new Expression();
        expression.setFunctionName(functionName);
        expression.setVarName(varName);
        expression.setValue(value);
        return expression;
    }
    public List<Expression> createExpression(JSONObject msg){
        List<Expression> expressions=new ArrayList<>();
        for(String var:var2ExpressionList.keySet()){
            if(!msg.containsKey(var)){
                return null;
            }
            List<Expression> expressionList=createExpression(var);
            if(expressionList!=null){
                expressions.addAll(expressionList);
            }

        }
        if(expressions.size()==0){
            return null;
        }
        return expressions;
    }
    public List<Expression> createExpression(String varName){
       return this.var2ExpressionList.get(varName);

    }

    public Collection<String> getAllVars() {
        return var2ExpressionList.keySet();
    }

    public int getRuleId() {
        return ruleId;
    }
}
