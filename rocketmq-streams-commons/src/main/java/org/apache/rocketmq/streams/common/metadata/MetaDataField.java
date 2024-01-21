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
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.datatype.BooleanDataType;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.FloatDataType;
import org.apache.rocketmq.streams.common.datatype.IJsonable;
import org.apache.rocketmq.streams.common.datatype.IntDataType;
import org.apache.rocketmq.streams.common.datatype.LongDataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

public class MetaDataField<T> extends Entity implements IJsonable {

    public static final String ORIG_MESSAGE = "message";
    public static final String RULE = "rule";

    private static final long serialVersionUID = 3590425799189771820L;
    private String fieldName;
    private DataType<T> dataType;
    private Boolean isRequired;
    private Boolean isPrimary;//add by wangtl 20171103 增加主键支持，刈刀任务引擎生成建表语句
    // 前端处理使用
    @Deprecated
    private String dataTypeStr;

    public static DataType<?> getDataTypeByStr(String dataType) {
        DataType<?> dt = null;
        if ("String".equals(dataType)) {
            dt = new StringDataType();
        } else if ("long".equals(dataType)) {
            dt = new LongDataType();
        } else if ("int".equals(dataType)) {
            dt = new IntDataType();
        } else if ("float".equals(dataType)) {
            dt = new FloatDataType();
        } else if ("boolean".equals(dataType)) {
            dt = new BooleanDataType();
        } else {
            dt = new StringDataType();
        }
        return dt;
    }

    public static String getDataTypeStrByType(DataType<?> dataType) {
        String dataTypeStr = "";
        if (dataType instanceof StringDataType) {
            dataTypeStr = "String";
        } else if (dataType instanceof LongDataType) {
            dataTypeStr = "long";
        } else if (dataType instanceof IntDataType) {
            dataTypeStr = "int";
        } else if (dataType instanceof FloatDataType) {
            dataTypeStr = "float";
        } else {
            dataTypeStr = "String";
        }
        return dataTypeStr;
    }

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public DataType<T> getDataType() {
        return dataType;
    }

    public void setDataType(DataType<T> dataType) {
        this.dataType = dataType;
    }

    public String getDataTypeStr() {
        return dataTypeStr;
    }

    public void setDataTypeStr(String dataTypeStr) {
        this.dataTypeStr = dataTypeStr;
        DataType dt = MetaDataField.getDataTypeByStr(dataTypeStr);
        this.dataType = dt;
    }

    public void setRequired(String required) {
        if (required == null || "".equals(required)) {
            isRequired = true;
        }
        if ("true".equals(required)) {
            isRequired = true;
        } else if ("false".equals(required)) {
            isRequired = false;
        }

    }

    public void setPrimary(String primary) {
        if (primary == null || "".equals(primary)) {
            isPrimary = true;
        }
        if ("true".equals(primary)) {
            isRequired = true;
        } else if ("false".equals(primary)) {
            isRequired = false;
        }

    }

    public Boolean getIsRequired() {
        return isRequired;
    }

    public void setIsRequired(Boolean isRequired) {
        this.isRequired = isRequired;
    }

    public Boolean getIsPrimary() {
        return isPrimary;
    }

    public void setIsPrimary(Boolean isPrimary) {
        this.isPrimary = isPrimary;
    }

    @Override
    public String toJson() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("fieldName", fieldName);
        if (dataType == null) {
            dataType = (DataType) new StringDataType();
        }
        jsonObject.put("dataType", dataType.toJson());
        jsonObject.put("isRequired", isRequired);
        jsonObject.put("isPrimary", isPrimary);
        jsonObject.put(IConfigurable.CLASS_NAME,this.getClass().getName());
        return jsonObject.toJSONString();
    }

    @Override
    public void toObject(String jsonString) {
        JSONObject jsonObject = JSON.parseObject(jsonString);
        this.fieldName = jsonObject.getString("fieldName");
        String dataTypeJson = jsonObject.getString("dataType");
        this.dataType = DataTypeUtil.createDataType(dataTypeJson);
        this.isRequired = jsonObject.getBoolean("isRequired");
        this.isPrimary = jsonObject.getBoolean("isPrimary");
    }

}
