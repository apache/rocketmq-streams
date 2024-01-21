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
package org.apache.rocketmq.streams.common.topology.stages;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IDim;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class DimChainStage extends ScriptChainStage {
    protected IDim dim;
    protected String expressionStr;
    protected String alias;
    protected String dimScript;
    protected String[] fieldNames;
    protected boolean isInnerJoin;
    protected boolean isLeftJoin;
    protected String splitFieldName;

    @Override public void startJob() {
        dim.startLoadDimData();
    }

    @Override protected IMessage handleMessage(IMessage message, AbstractContext context) {
        JSONArray rows = getRows(message.getMessageBody(), expressionStr, alias, dimScript, fieldNames);
        if (isInnerJoin) {
            if (rows == null || rows.size() == 0) {
                context.breakExecute();
                return null;
            }
        } else if (isLeftJoin) {
            if (rows == null || rows.size() == 0) {
                return message;
            }
        }
        message.getMessageBody().put(splitFieldName, rows);

        return super.handleMessage(message, context);
    }

    /**
     * 根据表达式，从namelist中获取符合条件的数据
     *
     * @param expressionStr （varname,functionName,value)&(varname,functionName,value)
     * @param fieldNames    需要返回的字段名
     * @return
     */
    protected JSONArray getRows(JSONObject msg,
        String expressionStr, String alias, String script, String... fieldNames) {

        if (StringUtil.isEmpty(script)) {
            script = null;
        }
        List<Map<String, Object>> rows = this.dim.matchExpression(expressionStr, msg, true, script);
        if (rows == null || rows.size() == 0) {
            return null;
        }
        JSONArray jsonArray = new JSONArray();
        for (Map<String, Object> row : rows) {
            JSONObject jsonObject = new JSONObject();
            if (fieldNames == null || fieldNames.length == 0) {
                if (StringUtil.isEmpty(alias)) {
                    jsonObject.putAll(row);
                } else {
                    Iterator<Map.Entry<String, Object>> it = row.entrySet().iterator();
                    while (it.hasNext()) {
                        Map.Entry<String, Object> entry = it.next();
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

    @Override public void stopJob() {
        dim.destroy();
    }

    public IDim getDim() {
        return dim;
    }

    public void setDim(IDim dim) {
        this.dim = dim;
    }

    public String getExpressionStr() {
        return expressionStr;
    }

    public void setExpressionStr(String expressionStr) {
        this.expressionStr = expressionStr;
    }

    public String getAlias() {
        return alias;
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }

    public String getDimScript() {
        return dimScript;
    }

    public void setDimScript(String dimScript) {
        this.dimScript = dimScript;
    }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(String[] fieldNames) {
        this.fieldNames = fieldNames;
    }

    public boolean isInnerJoin() {
        return isInnerJoin;
    }

    public void setInnerJoin(boolean innerJoin) {
        isInnerJoin = innerJoin;
    }

    public boolean isLeftJoin() {
        return isLeftJoin;
    }

    public void setLeftJoin(boolean leftJoin) {
        isLeftJoin = leftJoin;
    }

    public String getSplitFieldName() {
        return splitFieldName;
    }

    public void setSplitFieldName(String splitFieldName) {
        this.splitFieldName = splitFieldName;
    }

    @Override public boolean isAsyncNode() {
        return false;
    }
}
