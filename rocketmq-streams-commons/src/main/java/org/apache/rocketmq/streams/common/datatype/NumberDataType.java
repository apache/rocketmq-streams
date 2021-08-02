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
package org.apache.rocketmq.streams.common.datatype;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class NumberDataType extends BaseDataType<Number> {

    private static final long serialVersionUID = -9201119260447517832L;

    public NumberDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public NumberDataType() {
        setDataClazz(Number.class);
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    public String getDataTypeName() {
        return "number";
    }

    @Override
    public boolean matchClass(Class clazz) {
        return Number.class.getSimpleName().equals(clazz.getSimpleName());
    }

    @Override
    public String toDataJson(Number value) {
        if (value == null) {
            return null;
        }
        Number result = NumberUtils.stripTrailingZeros(value.doubleValue());
        if (result instanceof Integer) {
            return String.valueOf(value.intValue());
        } else {
            return String.valueOf(value.doubleValue());
        }
    }

    @Override
    public Number getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        try {
            return NumberUtils.stripTrailingZeros(Double.valueOf(jsonValue));
        } catch (Exception e) {
            return null;
        }
    }
}
