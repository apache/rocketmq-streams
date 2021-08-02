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
package org.apache.rocketmq.streams.filter.operator.action.impl;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;
import org.apache.rocketmq.streams.filter.operator.var.Var;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataAdapter;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MetaDataAction extends Action<Boolean> {

    private static final Log LOG = LogFactory.getLog(MetaDataAction.class);
    protected String metaDataName;
    protected Map<String, String> varName2FieldName;

    @Override
    public Boolean doAction(RuleContext context, Rule rule) {
        if (!volidate(context, rule)) {
            return null;
        }
        Map<String, Object> fieldName2Value = getQuery(context, rule);

        //        /**
        //         * 为开启killchain的规则设置killchainId
        //         */
        //        if (rule.getKillChainFlag() != null && rule.getKillChainFlag().intValue() == 1) {
        //            fieldName2Value.put("killchainId", System.currentTimeMillis() + Math.round(Math.random() * 9999));
        //        }
        Boolean result = doAction(context, rule, fieldName2Value);
        return result;
    }

    protected Boolean doAction(RuleContext context, Rule rule, Map<String, Object> fieldName2Value) {
        MetaDataAdapter metaDataAdapter = context.getMetaDataAdapter(metaDataName);
        if (metaDataAdapter == null) {
            throw new RuntimeException("MetaDataAction doAction metaDataAdapter is null");
        }
        return metaDataAdapter.insert(fieldName2Value);
    }

    public String getMetaDataName() {
        return metaDataName;
    }

    public void setMetaDataName(String metaDataName) {
        this.metaDataName = metaDataName;
    }

    public Map<String, String> getVarName2FieldName() {
        return varName2FieldName;
    }

    public void setVarName2FieldName(Map<String, String> varName2FieldName) {
        this.varName2FieldName = varName2FieldName;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        if (StringUtil.isEmpty(metaDataName)) {
            return false;
        }
        MetaDataAdapter adapter = context.getMetaDataAdapter(metaDataName);
        if (adapter == null || adapter.getMetaData() == null) {
            return false;
        }
        if (varName2FieldName == null || varName2FieldName.size() == 0) {
            return false;
        }
        return true;
    }

    /**
     * 查询数据
     *
     * @param context
     * @param rule
     * @return
     */
    public Map<String, Object> getQuery(RuleContext context, Rule rule) {
        Map<String, Object> query = new HashMap<String, Object>();
        MetaData metaData = context.getMetaData(metaDataName);
        Iterator<Map.Entry<String, String>> it = varName2FieldName.entrySet().iterator();
        while (it.hasNext()) {
            try {
                Map.Entry<String, String> entry = it.next();
                String varName = entry.getKey();
                String fieldName = entry.getValue();
                Var var = context.getVar(rule.getConfigureName(), varName);
                List<String> values = new ArrayList<>();
                /**
                 * 需要MetaDataField配置最终输出的字段
                 */
                try {
                    MetaDataField field = metaData.getMetaDataField(fieldName);
                    if (field == null) {
                        continue;
                    }
                    DataType dataType = field.getDataType();
                    if (query.containsKey(fieldName)) {
                        Object value = query.get(fieldName);
                        if (DataTypeUtil.isList(value.getClass())) {
                            values = (List)value;
                        } else {
                            query.put(fieldName, values);
                            values.add(dataType.toDataJson(value));
                        }
                        values.add(dataType.toDataJson(var.getVarValue(context, rule)));
                    } else {
                        query.put(fieldName, var.getVarValue(context, rule));
                    }
                } catch (Exception e) {
                    LOG.error("MetaDataAction getQuery error,rule is :" + rule.getRuleCode() + " ,action is: "
                        + rule.getActionNames() + " ,metaDataName is: " + metaDataName + " ,varName is: " + varName
                        + " ,fieldName is: " + fieldName, e);
                }

            } catch (Exception e) {
                LOG.error("MetaDataAction getQuery error,rule :" + rule.getRuleCode() + " ,action is:"
                    + rule.getActionNames(), e);
            }

        }
        return query;
    }

    /**
     * 把action的结果打印出来
     *
     * @param context
     * @param rule
     * @return
     */
    public String toString(RuleContext context, Rule rule) {
        Map<String, Object> fieldName2Value = getQuery(context, rule);

        JSONObject jsonObject = new JSONObject();
        Iterator<Map.Entry<String, Object>> it = fieldName2Value.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String value = String.valueOf(entry.getValue());
            try {
                if (value.contains(":")) {
                    jsonObject.put(entry.getKey(), JSONObject.parseObject(value));
                } else {
                    jsonObject.put(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                jsonObject.put(entry.getKey(), entry.getValue());
            }

        }
        return jsonObject.toJSONString();
    }

}
