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
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.operator.AbstractScriptOperator;
import org.python.util.PythonInterpreter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 实现思路，通过INNER_MESSAG 把message的jsonobject传给python，python中直接操作jsonobject
 */
public class JPythonScriptOperator extends AbstractScriptOperator {
    private static final Logger LOGGER = LoggerFactory.getLogger(JPythonScriptOperator.class);
    protected transient PythonInterpreter interpreter;

    public static void main(String[] args) {
        JPythonScriptOperator pythonScript = new JPythonScriptOperator();
        pythonScript.setValue("_msg.put('age',18);");
        pythonScript.init();
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", "chris");
        Message message = new Message(jsonObject);

        pythonScript.doMessage(message, new FunctionContext(message));
        System.out.println(jsonObject);
    }

    @Override
    protected boolean initConfigurable() {
        try {
            super.initConfigurable();
            Properties props = new Properties();

            props.put("python.console.encoding", "UTF-8");
            props.put("python.security.respectJavaAccessibility", "false");
            props.put("python.import.site", "false");
            Properties preprops = System.getProperties();
            PythonInterpreter.initialize(props, preprops, new String[] {});

            // 实例化环境和代码执行
            interpreter = new PythonInterpreter();
            interpreter.exec("import sys");
            registFunction();
        } catch (Exception e) {
            LOGGER.error("jython init error " + getValue(), e);
            return false;
        }
        return true;
    }

    @Override
    public List<IMessage> doMessage(IMessage message, AbstractContext context) {
        interpreter.set(INNER_MESSAG, message.getMessageBody());
        interpreter.exec(getValue());
        List<IMessage> messages = new ArrayList<>();
        messages.add(message);
        return messages;
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
