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
package org.apache.rocketmq.streams.script.operator;

import java.lang.reflect.Method;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 脚本的抽象类
 */
public abstract class AbstractScriptOperator extends AbstractScript<JSONObject, FunctionContext> {

    /**
     * json object对应的key
     */
    protected static final String INNER_MESSAG = "_msg";
    protected String functionName;
    /**
     * 脚本内容，在本类等于value
     */
    protected transient String scriptValue;

    @Override
    protected boolean initConfigurable() {
        this.scriptValue = getValue();
        return true;
    }

    @Override
    public abstract List<IMessage> doMessage(IMessage message, AbstractContext context);

    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String functionName) {
        this.functionName = functionName;
    }

    /**
     * 在子类中使用，比如groovy，python在加载后，把自己注册成一个函数。注册的方法就是IStreamOperator对应的方法
     */
    protected void registFunction() {
        if (StringUtil.isEmpty(functionName)) {
            return;
        }
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        Method method = null;
        try {
            method = this.getClass().getMethod("doMessage", new Class[] {IMessage.class, AbstractContext.class});
        } catch (NoSuchMethodException e) {
            throw new RuntimeException("get executeScript method error ", e);
        }
        scriptComponent.getFunctionService().registeFunction(functionName, this, method);
    }
}

