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
package org.apache.rocketmq.streams.script.function.impl.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.gson.JsonArray;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class JsonCreatorFunction {

    @FunctionMethod(value = "json_merge", alias = "jsonMerge", comment = "根据字段来组合json")
    public String extraJsonByField(IMessage message, FunctionContext context,
                                   @FunctionParamter(value = "json", comment = "字段名列表") String jsonFieldName) {
        jsonFieldName = FunctionUtils.getValueString(message, context, jsonFieldName);
        JSONObject msg = message.getMessageBody().getJSONObject(jsonFieldName);
        message.getMessageBody().putAll(msg);
        return null;
    }

    @FunctionMethod(value = "json", alias = "toJson", comment = "根据字段来组合json")
    public Object convertJson(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "array", comment = "字段名列表") String fieldName) {
        Object value = FunctionUtils.getValue(message, context, fieldName);
        if (JSONObject.class.isInstance(value)) {
            return (JSONObject)value;
        } else if (JsonArray.class.isInstance(value)) {
            return (JsonArray)value;
        } else {
            String temp = (String)value;
            if (temp.startsWith("[") && temp.endsWith("]")) {
                return JSONObject.parseArray(temp);
            } else if (temp.startsWith("{") && temp.endsWith("}")) {
                return JSONObject.parseObject(temp);
            } else {
                return null;
            }
        }

    }

    @FunctionMethod(value = "json_generate_field", alias = "json_create_field", comment = "根据字段来组合json")
    public String extraJsonByField(IMessage message, FunctionContext context,
                                   @FunctionParamter(value = "array", comment = "字段名列表") String... fieldNames) {
        if (fieldNames == null) {
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = fieldNames[i];
            Object value = FunctionUtils.getValue(message, context, fieldName);
            if (value == null) {
                continue;
            }
            jsonObject.put(fieldName, value);
        }
        return jsonObject.toJSONString();
    }

    @Deprecated
    @FunctionMethod(value = "json_create", alias = "json_generate", comment = "根据两个字段定义一个<key，value>,"
        + "如name:yuanxiaodong,age:18对应的json为{name:yuanxiaodong,age:18}")
    public String extraJson(IMessage message, FunctionContext context,
                            @FunctionParamter(value = "array", comment = "格式如下，加引号代表常量否则代表字段名name:yuanxiaodong,age:18") String... kvs) {
        if (kvs == null) {
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < kvs.length; i++) {
            String values[] = kvs[i].split(":");
            if (values.length < 2) {
                continue;
            }
            Object fieldName = FunctionUtils.getValue(message, context, values[0]);
            Object value = FunctionUtils.getValue(message, context, values[1]);
            if (value == null || fieldName == null) {
                continue;
            }
            jsonObject.put(fieldName.toString(), value);
        }
        return jsonObject.toJSONString();
    }

    /**
     * kv 格式 key:name,key:name
     *
     * @param message
     * @param context
     * @param kvs
     * @return
     */
    @FunctionMethod(value = "jsonAdd", alias = "json_add", comment = "给已有的json增加元素")
    public String addElement(IMessage message, FunctionContext context,
                             @FunctionParamter(value = "string", comment = "现有json对应的字段名或常量") String jsonFiledName,
                             @FunctionParamter(value = "array", comment = "格式如下，加引号代表常量否则代表字段名name:yuanxiaodong,age:18") String... kvs) {
        if (kvs == null) {
            return null;
        }
        String jsonValue = FunctionUtils.getValueString(message, context, jsonFiledName);
        if (jsonValue == null) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(jsonValue);
        for (int i = 0; i < kvs.length; i++) {
            String values[] = kvs[i].split(":");
            if (values.length < 2) {
                continue;
            }
            Object fieldName = FunctionUtils.getValue(message, context, values[0]);
            Object value = FunctionUtils.getValue(message, context, values[1]);
            if (value == null || fieldName == null) {
                continue;
            }
            jsonObject.put(fieldName.toString(), value);
        }
        message.getMessageBody().put(jsonFiledName, jsonObject.toJSONString());
        return jsonObject.toJSONString();
    }

    /**
     * kv 格式 key:name,key:name
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "jsonRemove", alias = "json_remove", comment = "移除现有json的元素")
    public String removeElement(IMessage message, FunctionContext context,
                                @FunctionParamter(value = "string", comment = "现有json对应的字段名或常量") String jsonFiledName,
                                @FunctionParamter(value = "array", comment = "代表要移除key的字段名或常量列表") String... keyNames) {
        if (keyNames == null) {
            return null;
        }
        String jsonValue = message.getMessageBody().getString(jsonFiledName);
        if (jsonValue == null) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(jsonValue);
        for (int i = 0; i < keyNames.length; i++) {
            Object fieldName = FunctionUtils.getValue(message, context, keyNames[i]);
            if (fieldName == null) {
                continue;
            }
            jsonObject.remove(fieldName.toString());
        }
        message.getMessageBody().put(jsonFiledName, jsonObject.toJSONString());
        return jsonObject.toJSONString();
    }

    /**
     * kv 格式 key:name,key:name
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "jsonExpand", alias = "json_expand", comment = "展开一个json中的json")
    public void expandElement(IMessage message, FunctionContext context,
                              @FunctionParamter(value = "array", comment = "代表要移除key的字段名或常量列表") String jsonSubFieldName) {
        jsonSubFieldName = FunctionUtils.getValueString(message, context, jsonSubFieldName);
        String jsonValue = message.getMessageBody().getString(jsonSubFieldName);
        if (jsonValue == null) {
            return;
        }
        JSONObject subJson = JSON.parseObject(jsonValue);
        if (subJson != null) {
            message.getMessageBody().remove(jsonSubFieldName);
            message.getMessageBody().putAll(subJson);
        }
    }

    /**
     * kv 格式 key:name,key:name
     *
     * @param message
     * @param context
     * @return
     */
    @FunctionMethod(value = "jsonExpand", alias = "json_expand", comment = "展开一个json中的json")
    public String expandElement(IMessage message, FunctionContext context,
                                @FunctionParamter(value = "string", comment = "现有json对应的字段名或常量") String jsonFiledName,
                                @FunctionParamter(value = "array", comment = "代表要移除key的字段名或常量列表") String jsonSubFieldName) {
        jsonFiledName = FunctionUtils.getValueString(message, context, jsonSubFieldName);
        jsonSubFieldName = FunctionUtils.getValueString(message, context, jsonSubFieldName);
        String jsonValue = message.getMessageBody().getString(jsonFiledName);
        if (jsonValue == null) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(jsonValue);
        JSONObject subJson = jsonObject.getJSONObject(jsonSubFieldName);
        if (subJson != null) {
            jsonObject.remove(jsonSubFieldName);
            jsonObject.putAll(subJson);
        }
        message.getMessageBody().put(jsonFiledName, jsonObject.toJSONString());
        return jsonObject.toJSONString();
    }
}
