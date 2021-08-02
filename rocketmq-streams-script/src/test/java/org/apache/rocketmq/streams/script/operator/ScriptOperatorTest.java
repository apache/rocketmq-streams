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

import java.util.List;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class ScriptOperatorTest {

    @Test
    public void testScriptComponent() {
        JSONObject msg = new JSONObject();
        msg.put("modify_time", "1970-01-01  00:00:00");
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        scriptComponent.getService().executeScript(msg, "now=now();if(modify_time>=now){break='true';}else{break='false';};");
        assertTrue(msg.getString("break").equals("false"));
        System.out.println(msg);
    }

    @Test
    public void testScriptOperator() {
        JSONObject msg = new JSONObject();
        msg.put("modify_time", "1970-01-01  00:00:00");
        FunctionScript functionScript = new FunctionScript();
        functionScript.setValue("now=now();if(modify_time>=now){break='true';}else{break='false'};");
        functionScript.init();
        IMessage iMessage = new Message(msg);
        Context context = new Context(iMessage);
        List<IMessage> messageList = functionScript.doMessage(iMessage, context);
        for (IMessage message : messageList) {
            assertTrue(message.getMessageBody().getString("break").equals("false"));
            System.out.println(message.getMessageBody());
        }
    }

    /**
     * 测试拆分函数
     */
    @Test
    public void testSplit() {
        JSONArray jsonArray = createJsonArray();
        JSONObject msg = new JSONObject();
        msg.put("data", jsonArray);
        FunctionScript functionScript = new FunctionScript("splitArray('data');rm(data);now=now();");
        functionScript.init();
        IMessage iMessage = new Message(msg);
        Context context = new Context(iMessage);
        List<IMessage> messageList = functionScript.doMessage(iMessage, context);
        assertTrue(messageList.size() == 3);
        for (IMessage message : messageList) {
            System.out.println(message.getMessageBody());
        }
    }

    ////CREATE FUNCTION json_concat as 'com.aliyun.sec.lyra.udf.ext.JsonConcat' ;
    //@Test
    //public void testBlinkUDF(){
    //    BlinkUDFScript blinkUDFScript=new BlinkUDFScript();
    //    blinkUDFScript.setFunctionName("json_concat");
    //    blinkUDFScript.setFullClassName("com.aliyun.sec.lyra.udf.ext.JsonConcat");
    //    blinkUDFScript.init();
    //    JSONObject msg=new JSONObject();
    //    msg.put("a","123");
    //    msg.put("b","456");
    //    ScriptComponent scriptComponent=ScriptComponent.getInstance();
    //    scriptComponent.executeScript(msg,"value=json_concat('a',a,'b',b);");
    //    System.out.println(msg);
    //
    //}

    protected JSONArray createJsonArray() {
        JSONArray jsonArray = new JSONArray();
        for (int i = 0; i < 3; i++) {
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("name", "chris" + i);
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }
}
