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
package org.apache.rocketmq.streams.common.monitor;

import java.util.*;

/**
 * 数据流在哪里被过滤掉了
 */
public class TopologyFilterMonitor {

    protected String notFireRule;//未触发的规则
    //由于哪个表达式，导致的规则失败，以及表达式依赖的变量
    protected Map<String, List<String>> notFireExpression2DependentFields = new HashMap<>();

    public void addNotFireExpression(String expression, List<String> dependentFields) {
        notFireExpression2DependentFields.put(expression, dependentFields);
    }

    public void addNotFireExpression(String expression, String dependentField) {
        List<String> dependentFields = new ArrayList<>();
        dependentFields.add(dependentField);
        notFireExpression2DependentFields.put(expression, dependentFields);
    }

    public void addNotFireExpression(String expression, Set<String> dependentFieldSet) {
        List<String> dependentFields = new ArrayList<>();
        dependentFields.addAll(dependentFieldSet);
        notFireExpression2DependentFields.put(expression, dependentFields);
    }

    public void addNotFireExpression(Map<String, List<String>> notFireExpression2DependentFieldMap) {
        this.notFireExpression2DependentFields.putAll(notFireExpression2DependentFieldMap);
    }

    public String getNotFireRule() {
        return notFireRule;
    }

    public void setNotFireRule(String notFireRule) {
        this.notFireRule = notFireRule;
    }

    public Map<String, List<String>> getNotFireExpression2DependentFields() {
        return notFireExpression2DependentFields;
    }

    public void setNotFireExpression2DependentFields(
        Map<String, List<String>> notFireExpression2DependentFields) {
        this.notFireExpression2DependentFields = notFireExpression2DependentFields;
    }
}
