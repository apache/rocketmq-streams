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
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigurableDataType extends BaseDataType<IConfigurable> {

    /**
     *
     */
    private static final long serialVersionUID = -1298103769655414947L;

    private static final String VALUE_KEY = "configurable_value";

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurableDataType.class);

    public ConfigurableDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public ConfigurableDataType() {

    }

    public static String getTypeName() {
        return "configurable";
    }

    @Override
    public String toDataJson(IConfigurable value) {
        if (value == null) {
            return null;
        }
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(DATA_TYPE_CLASS_NAME, value.getClass().getName());
        jsonObject.put(VALUE_KEY, value.toJson());
        return jsonObject.toJSONString();
    }

    @Override
    public IConfigurable getData(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        JSONObject jsonObject = JSON.parseObject(jsonValue);
        String className = jsonObject.getString(DATA_TYPE_CLASS_NAME);
        IConfigurable configurable = createConfigurable(className);
        String valueJson = jsonObject.getString(VALUE_KEY);
        if (StringUtil.isEmpty(valueJson)) {
            configurable.toObject(jsonValue);
        } else {
            configurable.toObject(valueJson);
        }

        return configurable;
    }

    /**
     * 创建configurable对象
     *
     * @param className
     * @return
     */
    private IConfigurable createConfigurable(String className) {
        try {
            return (IConfigurable) Class.forName(className).newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String getName() {
        return IConfigurable.class.getSimpleName();
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

    @Override
    public boolean matchClass(Class clazz) {
        return IConfigurable.class.isAssignableFrom(clazz);
    }

    @Override
    public DataType create() {
        return new ConfigurableDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }
}
