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

public class StringDataType extends BaseDataType<String> {
    /**
     *
     */
    private static final long serialVersionUID = 5684373292535854114L;

    public StringDataType() {
        setDataClazz(String.class);
    }

    public StringDataType(Class clazz) {
        setDataClazz(clazz);
    }

    @Override
    public String getData(String jsonValue) {
        return jsonValue;
    }

    @Override
    public String getName() {
        return String.class.getSimpleName();
    }

    public static String getTypeName() {
        return "string";
    }

    @Override
    public String toDataJson(String value) {
        return value;
    }

    @Override
    public boolean matchClass(Class clazz) {
        return String.class.isAssignableFrom(clazz);
    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {String.class};
    }
}
