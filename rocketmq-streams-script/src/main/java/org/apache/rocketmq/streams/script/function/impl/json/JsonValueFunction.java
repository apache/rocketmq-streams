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
 */package org.apache.rocketmq.streams.script.function.impl.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.ScriptComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Function
public class JsonValueFunction {

    @FunctionMethod(value = "json",comment = "获取msg中的json数据")
    public JSONObject getJson(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String fieldName){
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        return message.getMessageBody().getJSONObject(fieldName);
    }

    @FunctionMethod(value = "json_value", alias = "json_get", comment = "获取json中的元素，支持$.方式")
    public Object extra(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonValueOrFieldName,
        @FunctionParamter(value = "string", comment = "获取json的模式，支持name.name的方式，支持数组$.name[index].name的方式")
            String path) {
        if (StringUtil.isEmpty(jsonValueOrFieldName) || StringUtil.isEmpty(path)) {
            return null;
        }
        String value = FunctionUtils.getValueString(message, context, jsonValueOrFieldName);
        String pattern = FunctionUtils.getValueString(message, context, path);
        if(pattern==null){
            pattern=path;
        }
        if (StringUtil.isEmpty(value) || StringUtil.isEmpty(pattern)) {
            return null;
        }
        if (pattern.startsWith("$.")) {
            pattern = pattern.substring(2);
        }
        Object bean = null;
        if (value.startsWith("[") && value.endsWith("]")) {
            bean = JSON.parseArray(value);
        } else {
            bean = JSON.parseObject(value);
        }
        return ReflectUtil.getBeanFieldOrJsonValue(bean, pattern);
    }

    public static void main(String[] args) {
        String jsonPattern="$.meta_conf.aliUid";
        JSONObject msg=new JSONObject();
        JSONObject metaConf=new JSONObject();
        metaConf.put("aliUid","23433223233");
        msg.put("meta_conf",metaConf);
        JSONObject jsonObject=new JSONObject();
        jsonObject.put("content",msg);
        ScriptComponent scriptComponent=ScriptComponent.getInstance();
        List<IMessage> msgs=scriptComponent.getService().executeScript(jsonObject,"aliuid=json_value(content,'$.meta_conf.aliUid');");
        for(IMessage message:msgs){
            System.out.println(message.getMessageBody());
        }
    }

    @FunctionMethod(value = "for_field", alias = "forField", comment = "循环所有的msg字段")
    public Object extra(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String scriptValue){
        scriptValue=FunctionUtils.getValueString(message,context,scriptValue);
        Iterator<Map.Entry<String, Object>> it = message.getMessageBody().entrySet().iterator();
        JSONObject msg=new JSONObject();
        msg.putAll(message.getMessageBody());
        while (it.hasNext()){
            Map.Entry<String, Object> entry=it.next();
            String key=entry.getKey();
            Object value=entry.getValue();
            msg.put("iterator.key",key);
            msg.put("iterator.value",value);
            ScriptComponent.getInstance().getService().executeScript(msg,scriptValue);
        }
        msg.remove("iterator.key");
        msg.remove("iterator.value");
        message.getMessageBody().clear();
        message.getMessageBody().putAll(msg);
        return null;
    }
}
