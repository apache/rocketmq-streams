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

package org.apache.rocketmq.streams.window.operator.join;
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

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.ScriptComponent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpressionMatcher {
    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it,
                                                                  String expressionStr, JSONObject msg, boolean needAll) {
        return matchExpressionByLoop(it, expressionStr, msg, needAll, null, new HashSet<>());
    }


    public static List<Map<String, Object>> matchExpressionByLoop(Iterator<Map<String, Object>> it,
                                                                  String expressionStr, JSONObject msg, boolean needAll, String script, Set<String> colunmNames) {
        List<Map<String, Object>> rows = new ArrayList<>();
        Rule ruleTemplete = ExpressionBuilder.createRule("tmp", "tmpRule", expressionStr);
        while (it.hasNext()) {
            Map<String, Object> oldRow = it.next();
            Map<String, Object> newRow = isMatch(ruleTemplete, oldRow, msg, script, colunmNames);
            if (newRow != null) {
                rows.add(newRow);
                if (!needAll) {
                    return rows;
                }
            }
        }
        return rows;
    }

    public static Map<String, Object> isMatch(Rule ruleTemplete, Map<String, Object> dimRow, JSONObject msgRow,
                                              String script, Set<String> colunmNames) {
        Map<String, Object> oldRow = dimRow;
        Map<String, Object> newRow = executeScript(oldRow, script);
        if (ruleTemplete == null) {
            return newRow;
        }
        Rule rule = ruleTemplete.copy();
        Map<String, Expression> expressionMap = new HashMap<>();
        String dimAsName = null;
        ;
        for (Expression expression : rule.getExpressionMap().values()) {
            expressionMap.put(expression.getConfigureName(), expression);
            if (expression instanceof RelationExpression) {
                continue;
            }
            Object object = expression.getValue();
            if (object != null && DataTypeUtil.isString(object.getClass())) {
                String fieldName = (String) object;
                Object value = newRow.get(fieldName);
                if (value != null) {
                    Expression e = expression.copy();
                    e.setValue(value.toString());
                    expressionMap.put(e.getConfigureName(), e);
                }
            }
            if (expression.getVarName().contains(".")) {
                String[] values = expression.getVarName().split("\\.");
                if (values.length == 2) {
                    String asName = values[0];
                    String varName = values[1];
                    if (colunmNames.contains(varName)) {
                        dimAsName = asName;
                    }
                }

            }
        }
        rule.setExpressionMap(expressionMap);
        rule.initElements();
        JSONObject copyMsg = msgRow;
        if (StringUtil.isNotEmpty(dimAsName)) {
            copyMsg = new JSONObject(msgRow);
            for (String key : newRow.keySet()) {
                copyMsg.put(dimAsName + "." + key, newRow.get(key));
            }
        }
        boolean matched = rule.execute(copyMsg);
        if (matched) {
            return newRow;
        }
        return null;
    }

    protected static Map<String, Object> executeScript(Map<String, Object> oldRow, String script) {
        if (script == null) {
            return oldRow;
        }
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        msg.putAll(oldRow);
        scriptComponent.getService().executeScript(msg, script);
        return msg;
    }
}
