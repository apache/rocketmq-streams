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
package org.apache.rocketmq.streams.script.function.impl.parser;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;

import java.util.Map;

@Function
public class GrokFunction {
    private static GrokCompiler grokCompiler = GrokCompiler.newInstance();

    static {
        grokCompiler.registerDefaultPatterns();
    }

    @FunctionMethod(value = "grok", alias = "GROK")
    public JSONObject doGrok(IMessage message, AbstractContext context, String fieldName, String grokStr) {
        /*
         传入自定义的pattern, 会从已注册的patterns里面进行配对, 例如: TIMESTAMP_ISO8601:timestamp1, TIMESTAMP_ISO8601在注册的
         patterns里面有对应的解析格式, 配对成功后, 会在match时按照固定的解析格式将解析结果存入map中, 此处timestamp1作为输出的key
          */
        grokStr = FunctionUtils.getValueString(message, context, grokStr);
        Grok grok = grokCompiler.compile(grokStr);
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        String logMsg = message.getMessageBody().getString(fieldName);
        // 通过match()方法进行匹配, 对log进行解析, 按照指定的格式进行输出
        Match grokMatch = grok.match(logMsg);
        // 获取结果
        Map<String, Object> resultMap = grokMatch.capture();
        message.getMessageBody().putAll(resultMap);
        return message.getMessageBody();
    }

    @FunctionMethod(value = "add_grok", alias = "addGrok")
    public void addGrok(IMessage message, AbstractContext context, String name, String pattern) {
        name = FunctionUtils.getValueString(message, context, name);
        pattern = FunctionUtils.getValueString(message, context, pattern);
        grokCompiler.register(name, pattern);
    }

    public static void main(String[] args) {
        String log = "localhost GET /index.html 1024 0.016";
        String grok = "%{IPORHOST:client} %{WORD:method} %{URIPATHPARAM:request} %{INT:size} %{NUMBER:duration}";

        JSONObject jsonObject = new JSONObject();
        jsonObject.put("log", log);

        IMessage message = new Message(jsonObject);

        AbstractContext context = new Context(message);

        GrokFunction grokFunction = new GrokFunction();
        long start = System.currentTimeMillis();
        JSONObject value = grokFunction.doGrok(message, context, "'log'", "'" + grok + "'");
        //          log=LogParserUtil.parse(log);
        //        String[] values=log.split(" ");
        //        jsonObject.put("client",values[0]);
        //        jsonObject.put("method",values[1]);
        //        jsonObject.put("request",values[2]);
        //        jsonObject.put("size",values[3]);
        //        jsonObject.put("duration",values[4]);
        System.out.println(System.currentTimeMillis() - start);
        //        value.remove("log");
        System.out.println(value);
    }
}
