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

import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.function.function.JavaObjectUDFFunction;
import org.apache.rocketmq.streams.script.function.function.Person;
import org.apache.rocketmq.streams.script.function.model.FunctionType;
import org.junit.Test;

public class UDTFFunctionTest {

    @Test
    public void testJavaObjectUDFFunction() throws NoSuchMethodException {
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JavaObjectUDFFunction objectUDFFunction = new JavaObjectUDFFunction();
        Method method = objectUDFFunction.getClass().getMethod("eval", new Class[] {Person.class, boolean.class});

        if (List.class.isAssignableFrom(method.getReturnType())) {
            scriptComponent.getFunctionService().registeUserDefinedUDTFFunction("test_udf123", objectUDFFunction, method);
        } else {
            scriptComponent.getFunctionService().registeFunction("test_udf123", objectUDFFunction, method, FunctionType.UDF);
        }
        JSONObject msg = new JSONObject();
        msg.put("person", new Person("chris", 18));

        scriptComponent.getService().executeScript(msg, "name=test_udf123(person,'true');");
        System.out.println(msg);

    }

    @Test
    public void testJavaObjectUDTFFunction() throws NoSuchMethodException {
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JavaObjectUDFFunction objectUDFFunction = new JavaObjectUDFFunction();
        Method method = objectUDFFunction.getClass().getMethod("eval", new Class[] {Person.class, String.class});
        if (List.class.isAssignableFrom(method.getReturnType())) {
            scriptComponent.getFunctionService().registeUserDefinedUDTFFunction("test_udf123", objectUDFFunction, method);
        } else {
            scriptComponent.getFunctionService().registeFunction("test_udf123", objectUDFFunction, method, FunctionType.UDF);
        }
        JSONObject msg = new JSONObject();
        msg.put("person", new Person("chris;yidao;yuda;taoqian;zengyu", 18));

        List<IMessage> messageList = scriptComponent.getService().executeScript(msg, "name=test_udf123(person,';');");
        for (IMessage message : messageList) {
            System.out.println(message.getMessageBody());
        }
    }
}
