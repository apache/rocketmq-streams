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
package org.apache.rocketmq.streams.common.metadata;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.datatype.DataJsonable;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.ListDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

public abstract class AbstractMetaData<T> extends BasedConfigurable implements DataJsonable<Map<String, Object>> {

    public static final String TYPE = "metaData";
    private String nameSpace;
    private String type = TYPE;
    private String configureName;
    protected List<MetaDataField<T>> metaDataFields = new ArrayList<MetaDataField<T>>();
    protected String dataSourceName;
    private String tableName;
    private String tableNameAlias;

    public AbstractMetaData() {
        setType(TYPE);
    }

    protected transient Map<String, MetaDataField<T>> metaDataFieldMap = new HashMap<String, MetaDataField<T>>();

    public List<MetaDataField<T>> getMetaDataFields() {
        return metaDataFields;
    }

    public void setMetaDataFields(List<MetaDataField<T>> metaDataFields) {
        this.metaDataFields = metaDataFields;
    }

    public MetaDataField<T> getMetaDataField(String name) {
        if (metaDataFields != null && metaDataFields.size() > 0 && this.metaDataFieldMap.size() == 0) {
            synchronized (this) {
                if (metaDataFields != null && metaDataFields.size() > 0 && this.metaDataFieldMap.size() == 0) {
                    Map<String, MetaDataField<T>> map = new HashMap<>();
                    for (MetaDataField metaDataField : metaDataFields) {
                        map.put(metaDataField.getFieldName(), metaDataField);
                    }
                    this.metaDataFieldMap = map;
                }
            }
        }
        return metaDataFieldMap.get(name);
    }

    public void addMetaDataField(String name, String typeName, boolean isReqired) {
        MetaDataField metaDataField = new MetaDataField<T>();
        metaDataField.setFieldName(name);
        metaDataField.setIsRequired(isReqired);
        metaDataField.setDataType(DataTypeUtil.getDataType(typeName));
        this.metaDataFields.add(metaDataField);
    }

    @Override
    public String toDataJson(Map<String, Object> dataset) {
        JSONObject jsonObject = new JSONObject();
        Iterator<Map.Entry<String, Object>> it = dataset.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            MetaDataField<T> field = this.metaDataFieldMap.get(fieldName);
            if (field == null) {
                continue;
            }

            DataType dataType = field.getDataType();
            String valueJson = "";
            if (DataTypeUtil.isList(value.getClass())) {
                DataType listDataType = new ListDataType(List.class, dataType);
                valueJson = listDataType.toDataJson(value);
            } else {
                valueJson = dataType.toDataJson(value);
            }
            jsonObject.put(fieldName, valueJson);
        }
        return jsonObject.toJSONString();
    }

    @Override
    public Map<String, Object> getData(String jsonValue) {
        Map<String, Object> dataset = new HashMap<>();
        JSONObject jsonObject = JSON.parseObject(jsonValue);
        for (MetaDataField<T> field : this.metaDataFields) {
            String fieldName = field.getFieldName();
            if (!jsonObject.containsKey(fieldName)) {
                continue;
            }
            String valueJson = jsonObject.getString(fieldName);
            if (valueJson == null) {
                continue;
            }
            DataType dataType = field.getDataType();
            Object value = dataType.getData(valueJson);
            dataset.put(fieldName, value);
        }
        return dataset;
    }

    @Override
    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    @Override
    public void setConfigureName(String configureName) {
        this.configureName = configureName;
    }

    @Override
    public String getNameSpace() {
        return nameSpace;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public String getConfigureName() {
        return configureName;
    }

    public String getDataSourceName() {
        return dataSourceName;
    }

    public void setDataSourceName(String dataSourceName) {
        this.dataSourceName = dataSourceName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, MetaDataField<T>> getMetaDataFieldMap() {
        return metaDataFieldMap;
    }

    public void setMetaDataFieldMap(Map<String, MetaDataField<T>> metaDataFieldMap) {
        this.metaDataFieldMap = metaDataFieldMap;
    }

    public String getTableNameAlias() {
        return tableNameAlias;
    }

    public void setTableNameAlias(String tableNameAlias) {
        this.tableNameAlias = tableNameAlias;
    }

    @Override
    public String toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(IConfigurableService.CLASS_NAME, this.getClass().getName());
        JSONArray jsonArray = new JSONArray();
        Iterator i$ = this.metaDataFields.iterator();

        while (i$.hasNext()) {
            MetaDataField<T> field = (MetaDataField) i$.next();
            jsonArray.add(field.toJson());
        }

        jsonObject.put("dataSourceName", this.dataSourceName);
        jsonObject.put("tableName", this.tableName);
        jsonObject.put("tableNameAlias", this.tableNameAlias);
        jsonObject.put("metaDataFields", jsonArray.toJSONString());
//        this.setJsonValue(jsonObject);
        this.getJsonValue(jsonObject);
        return jsonObject.toJSONString();
    }

    protected abstract void setJsonValue(JSONObject var1);

    protected abstract void getJsonValue(JSONObject var1);

    @Override
    public void toObject(String jsonString) {
        JSONObject jsonObject = JSONObject.parseObject(jsonString);
        String jsonArrayString = jsonObject.getString("metaDataFields");
        JSONArray jsonArray = JSON.parseArray(jsonArrayString);
        List<MetaDataField<T>> resourceFields = new ArrayList();
        Map<String, MetaDataField<T>> resourceFieldMap = new HashMap();
        for (int i = 0; i < jsonArray.size(); ++i) {
            String fieldJson = jsonArray.getString(i);
            MetaDataField<T> metaDataField = new MetaDataField();
            metaDataField.toObject(fieldJson);
            String dataTypestr = "";

            try {
                if (metaDataField.getDataType() != null) {
                    dataTypestr = MetaDataField.getDataTypeStrByType(metaDataField.getDataType());
                }
            } catch (Exception var12) {
                dataTypestr = "String";
            }

            metaDataField.setDataTypeStr(dataTypestr);
            resourceFields.add(metaDataField);
            resourceFieldMap.put(metaDataField.getFieldName(), metaDataField);
        }

        this.dataSourceName = jsonObject.getString("dataSourceName");
        this.tableName = jsonObject.getString("tableName");
        this.tableNameAlias = jsonObject.getString("tableNameAlias");
        this.metaDataFields = resourceFields;
        this.metaDataFieldMap = resourceFieldMap;
//        this.getJsonValue(jsonObject);
        this.setJsonValue(jsonObject);

    }
}
