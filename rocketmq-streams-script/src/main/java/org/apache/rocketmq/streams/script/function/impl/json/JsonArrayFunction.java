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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class JsonArrayFunction {
    //    @FunctionMethod(value = "for_array", alias = "forArray", comment = "轮询jsonArray执行脚本")
    //    public Object forJsonArray(IMessage message, FunctionContext context,
    //        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonArrayFiedName,
    //        @FunctionParamter(value = "string", comment = "获取json的模式，支持name.name的方式，支持数组$.name[index].name的方式")
    //            String scriptValue) {
    //        JSONArray jsonArray = message.getMessageBody().getJSONArray(jsonArrayFiedName);
    //        String value = FunctionUtils.getValueString(message, context, scriptValue);
    //        if (StringUtil.isNotEmpty(value)) {
    //            scriptValue = value;
    //        }
    //        List<IMessage> messages =new ArrayList<>();
    //        for (int i = 0; i < jsonArray.size(); i++) {
    //            JSONObject msg = jsonArray.getJSONObject(i);
    //            List<IMessage> msgs = ScriptComponent.getInstance().executeScript(msg, scriptValue);
    //            if (msgs != null) {
    //                messages.addAll(msgs);
    //            }
    //        }
    //        message.getMessageBody().put(jsonArrayFiedName, messages);
    //        return null;
    //    }
    @FunctionMethod(value = "row2column", alias = "rowToColumn")
    public String convertRow2Column(IMessage message, FunctionContext context, String jsonArrayFieldName, String keyFieldName, String valueFieldName) {
        String fieldName = FunctionUtils.getValueString(message, context, jsonArrayFieldName);
        JSONArray jsonArray = message.getMessageBody().getJSONArray(fieldName);
        JSONObject jsonObject = new JSONObject();
        if (jsonArray == null || jsonArray.size() == 0) {
            return jsonObject.toJSONString();
        }
        keyFieldName = FunctionUtils.getValueString(message, context, keyFieldName);
        valueFieldName = FunctionUtils.getValueString(message, context, valueFieldName);
        for (int i = 0; i < jsonArray.size(); i++) {
            JSONObject row = jsonArray.getJSONObject(i);
            jsonObject.put(row.getString(keyFieldName), row.getString(valueFieldName));
        }
        return jsonObject.toJSONString();
    }

    @FunctionMethod(value = "in_max", alias = "max_from_array", comment = "获取jsonArray中的最大元素")
    public Object extraMax(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonValueOrFieldName,
        @FunctionParamter(value = "string", comment = "获取json的模式，支持name.name的方式，支持数组$.name[index].name的方式")
        String path) {
        return extraValue(message, context, jsonValueOrFieldName, path, true);
    }

    @FunctionMethod(value = "remove_max", alias = "rm_max", comment = "获取jsonArray中的最大元素")
    public Object removeMax(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonValueOrFieldName,
        @FunctionParamter(value = "string", comment = "获取json的模式，支持name.name的方式，支持数组$.name[index].name的方式") String path,
        @FunctionParamter(value = "boolean", comment = "去除最大值是否是排序") Boolean isOrder) {

        String maxValue = null;
        if (isOrder) {
            maxValue = extraValueOrder(message, context, jsonValueOrFieldName, path);
            return maxValue;
        } else {
            maxValue = extraValue(message, context, jsonValueOrFieldName, path, true);
        }

        String fieldName = FunctionUtils.getValueString(message, context, path);
        List jsonArray = (List) message.getMessageBody().get(jsonValueOrFieldName);
        JSONArray result = new JSONArray();
        for (int i = 0; i < jsonArray.size(); i++) {
            Object o = jsonArray.get(i);
            Map<String, Object> row = null;
            if (IMessage.class.isInstance(o)) {
                row = ((IMessage) o).getMessageBody();
            } else {
                row = (Map) o;
            }
            Object rowValue = row.get(fieldName);
            String fieldValue = null;
            if (rowValue != null) {
                fieldValue = rowValue.toString();
            }
            if (maxValue.equals(fieldValue)) {
                continue;
            }
            JSONObject jsonObject = new JSONObject();
            jsonObject.putAll(row);
            result.add(jsonObject);
        }
        message.getMessageBody().put(jsonValueOrFieldName, result);
        return maxValue;
    }

    @FunctionMethod(value = "in_min", alias = "min_from_array", comment = "获取jsonArray中的最小元素")
    public Object extraMin(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonValueOrFieldName,
        @FunctionParamter(value = "string", comment = "获取json的模式，支持name.name的方式，支持数组$.name[index].name的方式")
        String path) {
        return extraValue(message, context, jsonValueOrFieldName, path, false);
    }

    public String extraValue(IMessage message, FunctionContext context, String jsonValueOrFieldName, String path,
        boolean isMax) {
        if (StringUtil.isEmpty(jsonValueOrFieldName) || StringUtil.isEmpty(path)) {
            return null;
        }

        String fieldName = FunctionUtils.getValueString(message, context, path);
        List jsonArray = (List) message.getMessageBody().get(jsonValueOrFieldName);
        if (jsonArray == null) {
            return null;
        }
        String mValue = null;
        for (int i = 0; i < jsonArray.size(); i++) {
            Object o = jsonArray.get(i);
            Map<String, Object> row = null;
            if (IMessage.class.isInstance(o)) {
                row = ((IMessage) o).getMessageBody();
            } else {
                row = (Map) o;
            }
            Object rowValue = row.get(fieldName);
            String fieldValue = null;
            if (rowValue != null) {
                fieldValue = rowValue.toString();
            }
            if (mValue == null) {
                mValue = fieldValue;
            } else {
                if (isMax) {
                    if (mValue.compareTo(fieldValue) < 0) {
                        mValue = fieldValue;
                    }
                } else {
                    if (mValue.compareTo(fieldValue) > 0) {
                        mValue = fieldValue;
                    }
                }

            }
        }
        return mValue;
    }

    @FunctionMethod("max")
    public String extraValueOrder(IMessage message, FunctionContext context, String jsonValueOrFieldName, String path) {
        if (StringUtil.isEmpty(jsonValueOrFieldName) || StringUtil.isEmpty(path)) {
            return null;
        }
        String fieldName = FunctionUtils.getValueString(message, context, path);
        List jsonArray = (List) message.getMessageBody().get(jsonValueOrFieldName);
        if (jsonArray == null) {
            return null;
        }
        String maxValue = getListElementValue(jsonArray, jsonArray.size() - 1, fieldName);

        int length = jsonArray.size();
        int i = length - 2;
        for (; i > 0; i--) {
            String value = getListElementValue(jsonArray, i, fieldName);
            if (!value.equals(maxValue)) {
                break;
            }
        }
        List result = jsonArray.subList(0, i + 1);
        message.getMessageBody().put(jsonValueOrFieldName, result);
        return maxValue;
    }

    private String getListElementValue(List jsonArray, int i, String fieldName) {
        Object o = jsonArray.get(i);
        Map<String, Object> row = null;
        if (IMessage.class.isInstance(o)) {
            row = ((IMessage) o).getMessageBody();
        } else {
            row = (Map) o;
        }
        Object rowValue = row.get(fieldName);
        String fieldValue = null;
        if (rowValue != null) {
            fieldValue = rowValue.toString();
        }
        return fieldValue;
    }

    @FunctionMethod(value = "array_size", alias = "size_array", comment = "获取jsonArray的大小")
    public Integer arraySize(IMessage message, FunctionContext context,
        @FunctionParamter(value = "string", comment = "代表json的字段名或常量") String jsonValueOrFieldName) {
        if (StringUtil.isEmpty(jsonValueOrFieldName)) {
            return 0;
        }
        String tmp = jsonValueOrFieldName;
        jsonValueOrFieldName = FunctionUtils.getValueString(message, context, jsonValueOrFieldName);
        if (message.getMessageBody().get(jsonValueOrFieldName) == null) {
            jsonValueOrFieldName = tmp;
        }
        List value = (List) FunctionUtils.getValue(message, context, jsonValueOrFieldName);
        if (value == null) {
            return null;
        }
        return value.size();
    }
}
