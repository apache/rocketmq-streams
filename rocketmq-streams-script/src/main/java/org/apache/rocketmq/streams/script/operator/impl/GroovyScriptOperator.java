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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.AbstractScriptOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现思路，通过INNER_MESSAG 把message的jsonobject传给groovy，groovy中直接操作jsonobject
 */
public class GroovyScriptOperator extends AbstractScriptOperator {
    protected static final String GROOVY_NAME = "groovy";
    private static final Logger LOGGER = LoggerFactory.getLogger(GroovyScriptOperator.class);
    protected transient Invocable inv;
    protected transient ScriptEngine engine;

    public static void main(String[] args) throws ScriptException {
        //javax.operator.Bindings
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("age", 18);
        jsonObject.put("date", new Date());
        GroovyScriptOperator groovyScript = new GroovyScriptOperator();
        groovyScript.setValue("_msg.put(\"name\",'chris');");
        groovyScript.init();
        Message message = new Message(jsonObject);

        groovyScript.doMessage(message, new FunctionContext(message));
        System.out.println(jsonObject);
    }

    @Override
    protected boolean initConfigurable() {
        try {
            super.initConfigurable();
            ScriptEngineManager factory = new ScriptEngineManager();
            ScriptEngine engine = factory.getEngineByName(GROOVY_NAME);
            inv = (Invocable) engine;
            this.engine = engine;
            registFunction();
        } catch (Exception e) {
            LOGGER.error("groovy init error " + getValue(), e);
            return false;
        }
        return true;
    }

    @Override
    public List<IMessage> doMessage(IMessage message, AbstractContext context) {
        Bindings binding = engine.createBindings();
        binding.putAll(message.getMessageBody());
        binding.put(INNER_MESSAG, message.getMessageBody());
        executeGroovy(getValue(), binding);
        List<IMessage> messages = new ArrayList<>();
        messages.add(message);
        return messages;
    }

    protected String executeGroovy(String script, Bindings binding) {
        try {
            engine.eval(script, binding);
        } catch (ScriptException e) {
            throw new RuntimeException("execute groovy error,the operator is " + script, e);
        }
        return null;
    }

    @Override
    public List<String> getScriptsByDependentField(String fieldName) {
        throw new RuntimeException("can not support this method:getScriptsByDependentField");
    }

    @Override
    public Map<String, List<String>> getDependentFields() {
        return null;
    }
}
