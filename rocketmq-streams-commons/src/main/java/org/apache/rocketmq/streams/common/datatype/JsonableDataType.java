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
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class JsonableDataType extends BaseDataType {

    private static final long serialVersionUID = -3702352294350579534L;

    public JsonableDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public JsonableDataType() {

    }

    @Override
    public String toDataJson(Object value) {
        IJsonable jsonable = (IJsonable)value;
        String msg = jsonable.toJson();
        JSONObject jsonObject = JSON.parseObject(msg);
        jsonObject.put(IConfigurableService.CLASS_NAME, value.getClass().getName());
        return jsonObject.toJSONString();
    }

    @Override
    public Object getData(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        IJsonable jsonable = null;
        try {
            JSONObject jsonObject = JSON.parseObject(jsonValue);
            String className = jsonObject.getString(IConfigurableService.CLASS_NAME);
            jsonable = (IJsonable)ReflectUtil.forInstance(className);
            jsonable.toObject(jsonValue);
            return jsonable;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public boolean matchClass(Class clazz) {

        return IJsonable.class.isAssignableFrom(clazz);
    }

    @Override
    public DataType create() {
        return new JsonableDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    public String getName() {
        return "jsonable";
    }

    public static String getTypeName() {
        return "jsonable";
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    protected Class[] getSupportClass() {
        return null;
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }
}
