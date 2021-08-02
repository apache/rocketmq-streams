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
package org.apache.rocketmq.streams.filter.operator.var;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.StringDataType;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;

public class ConstantVar<T> extends Var<T> {

    private static final long serialVersionUID = 8490929155047073802L;
    protected T value;
    protected DataType<T> dataType;

    // 前端处理使用
    private String dataTypestr;

    @Override
    public T doAction(RuleContext context, Rule rule) {
        return value;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        if (value == null || dataType == null) {
            return false;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void getJsonObject(JSONObject jsonObject) {
        // value很多时候会是String类型，这样dataType.toDataJson会报错，所以先转为dataType类型
        if (dataType == null) {
            dataType = (DataType<T>)new StringDataType(String.class);
        }
        try {
            this.value = (T)dataType.getData(String.valueOf(value));
        } catch (Exception e) {
            e.printStackTrace();
        }

        jsonObject.put("value", dataType.toDataJson(value));
        jsonObject.put("dataType", dataType.toJson());
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setJsonObject(JSONObject jsonObject) {
        String dataTypeJson = jsonObject.getString("dataType");
        dataType = (DataType<T>)DataTypeUtil.createDataType(dataTypeJson);
        String valueString = jsonObject.getString("value");
        this.value = (T)dataType.getData(valueString);

        // 前端显示用
        String dataTypestr = "";
        try {
            if (dataType != null) {
                dataTypestr = MetaDataField.getDataTypeStrByType(dataType);
            }
        } catch (Exception e) {
            dataTypestr = "String";
        }
        this.dataTypestr = dataTypestr;
    }

    @Override
    public boolean canLazyLoad() {
        return false;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public DataType<T> getDataType() {
        return dataType;
    }

    public void setDataType(DataType<T> dataType) {
        this.dataType = dataType;
    }

    public String getDataTypestr() {
        return dataTypestr;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public void setDataTypestr(String dataTypestr) {
        this.dataTypestr = dataTypestr;
        DataType dt = MetaDataField.getDataTypeByStr(dataTypestr);
        this.dataType = dt;
    }

}
