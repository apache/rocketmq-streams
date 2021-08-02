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

import org.apache.rocketmq.streams.filter.operator.expression.Expression;

public class OptimizationExpression {
    protected Expression expression;
    protected String varName;
    protected Object value;
    protected String regex;

    public OptimizationExpression(Expression expression, String regex) {
        this.expression = expression;
        this.value = expression.getValue();
        this.varName = expression.getVarName();
        //if(regex.startsWith("^")){
        //    regex=regex.substring(1);
        //}
        //if(regex.endsWith("$")){
        //    regex=regex.substring(0,regex.length()-1);
        //}
        this.regex = regex;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Expression getExpression() {
        return expression;
    }

    public void setExpression(Expression expression) {
        this.expression = expression;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }
}
