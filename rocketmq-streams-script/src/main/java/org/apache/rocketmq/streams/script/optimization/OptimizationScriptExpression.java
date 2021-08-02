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
package org.apache.rocketmq.streams.script.optimization;

import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class OptimizationScriptExpression {
    protected String varName;//函数用到的计算字段名
    protected String regex;//函数返回的正则表达式
    protected String newFieldName;//函数返回的字段名
    protected IScriptExpression expression;
    protected boolean isNOT = false;//是否取反

    public OptimizationScriptExpression(String varName, String regex, String newFieldName, IScriptExpression expression, boolean isNOT) {
        this.varName = varName;
        this.regex = regex;
        this.newFieldName = newFieldName;
        this.expression = expression;
        this.isNOT = isNOT;
    }

    public IScriptExpression getExpression() {
        return expression;
    }

    public void setExpression(IScriptExpression expression) {
        this.expression = expression;
    }

    public String getVarName() {
        return varName;
    }

    public void setVarName(String varName) {
        this.varName = varName;
    }

    public String getRegex() {
        return regex;
    }

    public void setRegex(String regex) {
        this.regex = regex;
    }

    public String getNewFieldName() {
        return newFieldName;
    }

    public void setNewFieldName(String newFieldName) {
        this.newFieldName = newFieldName;
    }
}
