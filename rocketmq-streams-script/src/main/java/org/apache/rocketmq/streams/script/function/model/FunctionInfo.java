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
package org.apache.rocketmq.streams.script.function.model;

import java.util.Objects;

/**
 * 保存每个函数的参数和描述信息 主要用于展示
 */
public class FunctionInfo {
    private String params;//参数列表和参数类型
    private String comment;//函数的说明信息
    private String returnType;//返回的类型
    private String functionName;//函数名称
    private String group;//函数所属组

    public FunctionInfo(String functionName, String params, String comment, String returnType, String group) {
        this.functionName = functionName;
        this.params = params;
        this.comment = comment;
        this.returnType = returnType;
        this.group = group;
    }

    public String getParams() {
        return params;
    }

    public void setParams(String params) {
        this.params = params;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    public String getReturnType() {
        return returnType;
    }

    public void setReturnType(String returnType) {
        this.returnType = returnType;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionInfo that = (FunctionInfo) o;
        return Objects.equals(params, that.params) && Objects.equals(comment, that.comment) && Objects.equals(
            returnType, that.returnType) && Objects.equals(functionName, that.functionName) && Objects.equals(group,
            that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(params, comment, returnType, functionName, group);
    }
}
