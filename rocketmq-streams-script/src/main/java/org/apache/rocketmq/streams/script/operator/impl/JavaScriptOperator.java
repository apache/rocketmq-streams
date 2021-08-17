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
package org.apache.rocketmq.streams.script.operator.impl;

import org.apache.rocketmq.streams.common.compiler.CustomJavaCompiler;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.script.ScriptComponent;

/**
 * 可以传java源码，编译成类，提供服务 java源码的clas必须是无参的
 */
public class JavaScriptOperator extends BasedConfigurable {

    protected transient Object object;

    protected String javaCode;//java 源码字符串

    /**
     * 如果是true，会覆盖内置的同名函数，否则被覆盖。主要场景是先通过代码进行了函数扩展，后续又放到内置函数中的场景
     */
    protected boolean highPriority = false;

    @Override
    protected boolean initConfigurable() {
        boolean success = super.initConfigurable();
        CustomJavaCompiler customJavaCompiler = new CustomJavaCompiler(javaCode);
        Object object = customJavaCompiler.compileAndNewInstance();
        this.object = object;
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        scriptComponent.getFunctionService().registeFunction(object);
        return success;
    }

    public String getJavaCode() {
        return javaCode;
    }

    public void setJavaCode(String javaCode) {
        this.javaCode = javaCode;
    }

    public boolean isHighPriority() {
        return highPriority;
    }

    public void setHighPriority(boolean highPriority) {
        this.highPriority = highPriority;
    }
}
