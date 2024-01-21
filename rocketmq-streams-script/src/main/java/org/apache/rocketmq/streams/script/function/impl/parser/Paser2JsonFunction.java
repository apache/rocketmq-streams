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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class Paser2JsonFunction {
    /**
     * 如果原始数据是json，且希望能够自动展开成单层，可以调用这个方法，无论嵌套了几层json或jsonarray都会被展开
     *
     * @param message
     * @param context
     */
    @FunctionMethod(value = "spread_json", alias = "autoJson", comment = "原始数据是嵌套json或jsonArray调用此方法会自动展开成单层")
    public void spread2Json(IMessage message, FunctionContext context, String fieldName) {
        fieldName = FunctionUtils.getConstant(fieldName);

        JSONObject jsonObject = message.getMessageBody().getJSONObject(fieldName);
//        if (message.isJsonMessage()) {
//            jsonObject = message.getMessageBody();
//        }
        List<String> jsonArrayNames = new ArrayList<>();
        jsonObject = spreadJson(jsonObject, jsonArrayNames);
        if (jsonArrayNames.size() > 0) {
            List<JSONObject> jsonObjects = spreadJsonArray(jsonObject, jsonArrayNames);
            for (JSONObject tmp : jsonObjects) {
                IMessage copyMessage = message.copy();
                copyMessage.getMessageBody().putAll(tmp);
                context.getSplitMessages().add(copyMessage);
            }
            context.openSplitModel();
        } else {
            message.getMessageBody().putAll(jsonObject);
        }
    }

    @FunctionMethod(value = "paser2Json", alias = "toJson", comment = "原始数据转化为json")
    public String paser2Json(IMessage message, FunctionContext context) {
        if (message.isJsonMessage()) {
            return message.getMessageBody().toJSONString();
        }
        return null;
    }

    /**
     * 如果json中，确定数组的字段名，则把所有数组从内侧拉出来，如果多个数组，求笛卡尔积
     *
     * @param jsonObject          json对象
     * @param jsonArrayFieldNames 里面的数组字段名
     * @return
     */
    protected List<JSONObject> spreadJsonArray(JSONObject jsonObject, List<String> jsonArrayFieldNames) {

        if (jsonArrayFieldNames.size() == 0) {
            List<JSONObject> result = new ArrayList<>();
            result.add(jsonObject);
            return result;
        }
        JSONObject template = new JSONObject();
        template.putAll(jsonObject);
        for (String name : jsonArrayFieldNames) {
            template.remove(name);
        }

        List<JSONObject> cartesian = new ArrayList<>();
        List<String> arrayFieldNames = new ArrayList<>();
        boolean isFirst = true;
        for (String name : jsonArrayFieldNames) {//如果有多个数组，求笛卡尔积
            JSONArray jsonArray = jsonObject.getJSONArray(name);
            List<JSONObject> tmpList = cartesian;
            cartesian = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject element = jsonArray.getJSONObject(i);
                spreadJson(element, arrayFieldNames);
                if (isFirst) {
                    JSONObject merger = new JSONObject();
                    merger.putAll(template);
                    merger.putAll(element);
                    cartesian.add(merger);
                } else {
                    for (JSONObject cartesianJson : tmpList) {
                        JSONObject copy = new JSONObject();
                        copy.putAll(cartesianJson);
                        copy.putAll(element);
                        cartesian.add(copy);
                    }
                }
            }
            if (isFirst) {
                isFirst = false;
            }
        }
        if (arrayFieldNames == null || arrayFieldNames.size() == 0) {
            return cartesian;
        }
        //如果解析完，还有嵌套数组，则继续做一轮解析
        List<JSONObject> result = new ArrayList<>();
        for (JSONObject tmp : cartesian) {
            List<JSONObject> temp = spreadJsonArray(tmp, arrayFieldNames);
            result.addAll(temp);
        }
        return result;
    }

    /**
     * 无论嵌套多少层，完成json拉平
     *
     * @param jsonObject
     * @return
     */
    protected JSONObject spreadJson(JSONObject jsonObject, List<String> jsonArrayFieldNames) {
        JSONObject result = new JSONObject();
        boolean hasJson = false;

        List<String> names = new ArrayList<>();
        Iterator<Map.Entry<String, Object>> it = jsonObject.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String key = entry.getKey();
            Object object = entry.getValue();
            if (JSONArray.class.isInstance(object)) {
                if (jsonArrayFieldNames != null) {
                    jsonArrayFieldNames.add(key);
                }
            } else if (JSONObject.class.isInstance(object)) {
                JSONObject value = (JSONObject) object;
                value = spreadJson(value, jsonArrayFieldNames);
                hasJson = true;
                result.putAll(value);
                names.add(key);
            }
        }
        if (hasJson) {
            for (String key : names) {
                jsonObject.remove(key);
                jsonObject.putAll(result);
            }
        }
        return jsonObject;
    }

}
