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

public class NotSupportDataType extends BaseDataType {

    /**
     *
     */
    private static final long serialVersionUID = -6281339761952292694L;

    public NotSupportDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public NotSupportDataType() {
    }

    @Override
    public String toDataJson(Object value) {
        throw new RuntimeException("not support datatype, the object is " + value);
    }

    @Override
    public Object getData(String jsonValue) {
        throw new RuntimeException("not support datatype, the jsonValue is " + jsonValue);
    }

    @Override
    public String getName() {
        return "notSupport";
    }

    @Override
    public boolean matchClass(Class clazz) {
        return true;
    }

    @Override
    public DataType create() {
        return new NotSupportDataType();
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    public String getDataTypeName() {
        return getName();
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }
}
