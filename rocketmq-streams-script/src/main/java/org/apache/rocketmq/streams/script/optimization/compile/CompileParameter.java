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
package org.apache.rocketmq.streams.script.optimization.compile;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.expression.ScriptParameter;
import org.apache.rocketmq.streams.script.service.IScriptParamter;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 根据运行时的信息，对ScriptParameter进行优化，使其能在多数场景更快
 */
public class CompileParameter {

    protected IScriptParamter scriptParamter;

    protected boolean isField = false;//是否是字段，在needcontext中 使用

    protected boolean isSimpleParameter = true;//只有left有值
    protected Object leftValue;//left对应的值

    protected boolean needContext;//是否有context和message的初始化参数

    /**
     * 分为两种函数： 1.有message和context前缀的：如果不是嵌套函数，leftVar就是对应的参数，具体的值在函数中通过functionutil获取 2.无message和context前缀的：可能的值，常量，long,doule,boolean和字段值，除了字段值，都是可以预知的
     *
     * @param iscriptParamter
     */
    public CompileParameter(IScriptParamter iscriptParamter, boolean needContext) {
        this.scriptParamter = iscriptParamter;
        this.needContext = needContext;
        if (!ScriptParameter.class.isInstance(iscriptParamter)) {
            isSimpleParameter = false;
            return;
        }
        ScriptParameter scriptParameter = (ScriptParameter)iscriptParamter;
        if (scriptParameter.getRigthVarName() != null || scriptParameter.getFunctionName() != null) {
            isSimpleParameter = false;
            return;
        }
        this.leftValue = scriptParameter.getLeftVarName();
        if (needContext) {
            return;
        }
        if (!String.class.isInstance(this.leftValue)) {
            return;
        }
        String varName = (String)this.leftValue;
        if (FunctionUtils.isConstant(varName)) {
            this.leftValue = FunctionUtils.getConstant(varName);
        } else if (FunctionUtils.isLong(varName)) {
            this.leftValue = FunctionUtils.getLong(varName);
        } else if (FunctionUtils.isDouble(varName)) {
            this.leftValue = FunctionUtils.getDouble(varName);
        } else if (FunctionUtils.isBoolean(varName)) {
            this.leftValue = FunctionUtils.getBoolean(varName);
        } else {
            isField = true;
        }
    }

    /**
     * 获取参数的值
     *
     * @param message
     * @param context
     * @return
     */
    public Object getValue(IMessage message, FunctionContext context) {
        //如果是嵌套函数，则用原来的逻辑处理
        if (isSimpleParameter == false) {
            Object value = scriptParamter.getScriptParamter(message, context);
            if (needContext || !String.class.isInstance(value)) {
                return value;
            }
            String str = (String)value;
            Object object = FunctionUtils.getValue(message, context, str);
            return object;
        }
        /**
         * 如果是无前缀，且是字段名，则获取具体字段
         */
        if (needContext == false && isField) {
            return FunctionUtils.getFiledValue(message, context, (String)this.leftValue);
        }
        return this.leftValue;
    }

    /**
     * 字段是不是固定的
     *
     * @return
     */
    public boolean isFixedValue() {
        if (isSimpleParameter == false) {
            return false;
        }
        if (isField && !needContext) {
            return false;
        }
        return true;
    }

    public boolean isField() {
        return isField;
    }

    public void setField(boolean field) {
        isField = field;
    }

    public static void main(String[] args) {
        String var = "abdf";
        System.out.println(var.indexOf("."));
    }

}
