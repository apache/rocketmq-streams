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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class JavaBeanDataType extends BaseDataType {

    /**
     *
     */
    private static final long serialVersionUID = 8749848859175999778L;

    public JavaBeanDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public JavaBeanDataType() {

    }

    @Override
    public String toDataJson(Object value) {
        return JSON.toJSONString(value);
    }

    @Override
    public Object getData(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        return JSON.parse(jsonValue);
    }

    @Override
    public String getName() {
        return "javaBean";
    }

    public static String getTypeName() {
        return "javaBean";
    }

    @Override
    public boolean matchClass(Class clazz) {
        if (dataClazz == null) {
            return false;
        }
        return this.dataClazz.isAssignableFrom(clazz);
    }

    @Override
    public DataType create() {
        return new JavaBeanDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    protected Class[] getSupportClass() {
        return null;
    }
}
