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
package org.apache.rocketmq.streams.script.function;

import java.util.Date;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.operator.impl.JPythonScriptOperator;
import org.junit.Test;

public class JPythonFunctionTest {
    private ScriptComponent scriptComponent = ScriptComponent.getInstance();

    @Test
    public void testJython() {
        JPythonScriptOperator groovyScript = new JPythonScriptOperator();
        groovyScript.setValue("_inner_msg.put('name','chris');");
        groovyScript.setFunctionName("addName");
        groovyScript.init();
        JSONObject msg = new JSONObject();
        msg.put("data", new Date());
        long start = System.currentTimeMillis();
        String scriptValue = "py('_msg.put(\"name\",\"chris\");');";
        List<IMessage> list = scriptComponent.getService().executeScript(msg, scriptValue);
        for (int i = 0; i < list.size(); i++) {
            System.out.println(list.get(i).getMessageBody());
        }
    }

}
