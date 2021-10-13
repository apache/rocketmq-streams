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
package org.apache.rocketmq.streams.filter.function.script;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class CaseFunction {




    public static boolean isCaseFunction(String functionName){
        if("if".equals(functionName)||"case".equals(functionName)){
            return true;
        }
        return false;
    }

    /**
     * if(((((___compare_1&___compare_2)&___compare_3)&___compare_4)&___in_1)){___case_1='可疑编码命令';};
     *
     * @param message
     * @param context
     * @param value
     * @return
     */
    @FunctionMethod(value = "if", alias = "case", comment = "支持内嵌函数")
    public Boolean match(IMessage message, FunctionContext context, String value) {
        String tmp = value;
        value = FunctionUtils.getValueString(message, context, value);
        if (value == null) {
            value = tmp;
        }
        if (value.length() <= 5) {
            String lowValue = value.trim().toLowerCase();
            if (lowValue.equals("true") || lowValue.equals("false")) {
                return Boolean.valueOf(value);
            }
        }
        if (value.startsWith("(") && value.endsWith(")")) {
            String expression = value;
            Boolean result = ExpressionBuilder.executeExecute(System.currentTimeMillis() + "", expression,
               message,context);
            //message.getHeader().getRegex2Value().put(value,result);
            return result;
        }
        return false;
    }

    @FunctionMethod(value = "!", alias = "!if", comment = "支持内嵌函数")
    public Boolean notma(IMessage message, FunctionContext context, String value) {
        return !match(message, context, value);
    }

    //(___in_2&((___compare_5|__fdfddf)|___compare_7))
    //(___in_2&((___compare_5|(a,==,b))|___compare_7))
    public static void main(String[] args) {
        String scriptValue
            = "if((___in_2&((___compare_5|___compare_6)|___compare_7))){___case_1=___cast_java应用执行可疑指令_1;};";
        ScriptComponent scriptComponent = ScriptComponent.getInstance();
        JSONObject msg = new JSONObject();
        msg.put("___in_2", true);
        msg.put("___compare_5", true);
        msg.put("___compare_6", true);
        msg.put("___compare_7", false);
        msg.put("___cast_java应用执行可疑指令_1", "fdsfdsdsff");
        List<IMessage> messages = scriptComponent.getService().executeScript(msg, scriptValue);
        System.out.println(messages.get(0).getMessageBody().toJSONString());
    }

}
