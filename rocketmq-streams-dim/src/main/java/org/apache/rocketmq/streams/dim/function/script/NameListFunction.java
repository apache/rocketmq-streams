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
package org.apache.rocketmq.streams.dim.function.script;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.dim.DimComponent;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class NameListFunction {


    private DimComponent nameListComponent;

    @FunctionMethod(value = "innerJoin", alias = "inner_join")
    public JSONArray innerJoin(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String alias, String script, String... fieldNames) {
        JSONArray rows = getRows(message, context, namespace, nameListName, expressionStr, alias, script, fieldNames);
        if (rows == null || rows.size() == 0) {
            context.breakExecute();
            return null;
        }
        return rows;
    }

    @FunctionMethod(value = "leftJoin", alias = "left_join")
    public JSONArray leftJoin(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String alias, String script, String... fieldNames) {
        JSONArray rows = getRows(message, context, namespace, nameListName, expressionStr, alias, script, fieldNames);
        if (rows == null || rows.size() == 0) {
            return null;
        }
        return rows;
    }

    @FunctionMethod(value = "mark_rows", alias = "namelist_rows")
    public String namelist(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String... fieldNames) {
        JSONArray rows = getRows(message, context, namespace, nameListName, expressionStr, null, null, fieldNames);
        if (rows == null || rows.size() == 0) {
            return null;
        }
        return rows.toJSONString();
    }

    @FunctionMethod(value = "mark", alias = "namelist")
    public String namelist(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String fieldName) {
        String tmp = fieldName;
        nameListName = FunctionUtils.getValueString(message, context, nameListName);
        namespace = FunctionUtils.getValueString(message, context, namespace);
        expressionStr = FunctionUtils.getValueString(message, context, expressionStr);
        fieldName = FunctionUtils.getValueString(message, context, fieldName);
        if (fieldName == null) {
            fieldName = tmp;
        }
        nameListComponent = ComponentCreator.getComponent(namespace, DimComponent.class);
        Map<String, Object> row =
            nameListComponent.getService().match(nameListName, expressionStr, message.getMessageBody());
        if (row != null) {
            Object value = row.get(fieldName);
            if (value == null) {
                return null;
            }
            return value.toString();
        }
        return null;
    }

    @FunctionMethod(value = "mark", alias = "namelist")
    public String namelist(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String fieldNames, String joinMark) {
        nameListName = FunctionUtils.getValueString(message, context, nameListName);
        namespace = FunctionUtils.getValueString(message, context, namespace);
        expressionStr = FunctionUtils.getValueString(message, context, expressionStr);
        fieldNames = FunctionUtils.getValueString(message, context, fieldNames);
        joinMark = FunctionUtils.getValueString(message, context, joinMark);
        nameListComponent = ComponentCreator.getComponent(namespace, DimComponent.class);
        Map<String, Object> row =
            nameListComponent.getService().match(nameListName, expressionStr, message.getMessageBody());
        if (row != null) {
            String[] fieldNameTem = fieldNames.split(",");
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < fieldNameTem.length; i++) {
                Object tem = row.get(fieldNameTem[i]);
                if (tem != null) {
                    if (result.length() == 0) {
                        if (StringUtil.isNotEmpty(tem.toString()) && !("null".equalsIgnoreCase(tem.toString()))) {
                            result.append(tem);
                        }
                    } else {
                        if (StringUtil.isNotEmpty(tem.toString()) && !("null".equalsIgnoreCase(tem.toString()))) {
                            result.append(joinMark + tem);
                        }
                    }
                }

            }
            return result.toString();
        }
        return null;
    }

    @FunctionMethod(value = "in_namelist", alias = "in_namelist")
    public Boolean inNameList(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr) {
        nameListComponent = ComponentCreator.getComponent(namespace, DimComponent.class);
        Map<String, Object> row =
            nameListComponent.getService().match(nameListName, expressionStr, message.getMessageBody());
        if (row != null && row.size() > 0) {
            return true;
        }
        return false;
    }

    /**
     * 根据表达式，从namelist中获取符合条件的数据
     *
     * @param message
     * @param context
     * @param namespace
     * @param nameListName
     * @param expressionStr （varname,functionName,value)&(varname,functionName,value)
     * @param fieldNames    需要返回的字段名
     * @return
     */
    protected JSONArray getRows(IMessage message, FunctionContext context, String namespace, String nameListName,
        String expressionStr, String alias, String script, String... fieldNames) {
        nameListName = FunctionUtils.getValueString(message, context, nameListName);
        namespace = FunctionUtils.getValueString(message, context, namespace);
        expressionStr = FunctionUtils.getValueString(message, context, expressionStr);
        script = FunctionUtils.getValueString(message, context, script);
        if (StringUtil.isEmpty(script)) {
            script = null;
        }
        nameListComponent = ComponentCreator.getComponent(namespace, DimComponent.class);
        List<Map<String, Object>> rows =
            nameListComponent.getService().matchSupportMultiRow(nameListName, expressionStr, message.getMessageBody(), script);
        if (rows == null || rows.size() == 0) {
            return null;
        }
        JSONArray jsonArray = new JSONArray();
        for (Map<String, Object> row : rows) {
            JSONObject jsonObject = new JSONObject();
            if (fieldNames == null || fieldNames.length == 0) {
                if (StringUtil.isEmpty(FunctionUtils.getConstant(alias))) {
                    jsonObject.putAll(row);
                } else {
                    Iterator<Entry<String, Object>> it = row.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Object> entry = it.next();
                        String fieldName = entry.getKey();
                        if (alias != null) {
                            fieldName = alias + "." + fieldName;
                        }
                        jsonObject.put(fieldName, entry.getValue());
                    }
                }

            } else {
                for (String fieldName : fieldNames) {
                    String tmp = fieldName;
                    if (alias != null) {
                        fieldName = alias + "." + fieldName;
                    }
                    jsonObject.put(fieldName, row.get(tmp));
                }
            }
            jsonArray.add(jsonObject);
        }
        return jsonArray;
    }

}