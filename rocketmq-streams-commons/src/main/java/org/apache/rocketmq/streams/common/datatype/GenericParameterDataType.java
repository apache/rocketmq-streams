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
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class GenericParameterDataType<T> extends BaseDataType<T> {
    protected static final String DATATYPE_NAME = "datatypename";
    protected static final String ELEMENT_VALUE = "element_value";
    protected static final String ELEMENT_CLASS_NAME = "element_class_name";
    protected String genericParameterStr;

    public abstract void parseGenericParameter(String genericParameterString);

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {
        if (genericParameterStr != null) {
            jsonObject.put(GENERIC_PARAMETER, genericParameterStr);
        }
    }

    public abstract String toDataStr(T t);

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {
        this.genericParameterStr = jsonObject.getString(GENERIC_PARAMETER);
        if (StringUtil.isNotEmpty(this.genericParameterStr)) {
            this.genericParameterStr = this.genericParameterStr.trim();
        }
        parseGenericParameter(this.genericParameterStr);
    }

    @Override
    protected Class[] getSupportClass() {
        return null;
    }

    protected String createGenericParameterStr(DataType paradigmType) {
        if (paradigmType == null) {
            return null;
        }
        String subStr = null;
        if (paradigmType instanceof GenericParameterDataType) {
            GenericParameterDataType genericParameterDataType = (GenericParameterDataType) paradigmType;
            subStr = genericParameterDataType.createGenericParameterStr();
        } else {
            subStr = paradigmType.getDataClass().getName();
        }
        return subStr;
    }

    protected Object createObjectValue(DataType paradigmType, String json) {
        return createObjectValue(paradigmType, json, null);
    }

    protected Object createObjectValue(DataType paradigmType, String json, Set<String> className) {
        Object result = json;
        if (paradigmType == null) {
            JSONObject jsonObject = JSON.parseObject(json);
            String datatypeName = jsonObject.getString(DATATYPE_NAME);
            String data = jsonObject.getString(ELEMENT_VALUE);
            DataType dataType = DataTypeUtil.getDataType(datatypeName);
            if (className != null) {
                className.add(jsonObject.getString(ELEMENT_CLASS_NAME));
            }
            result = dataType.getData(data);
        } else {
            result = paradigmType.getData(json);
        }
        return result;
    }

    protected byte[] createByteValue(DataType paradigmType, Object o) {
        byte[] bytes = null;
        if (paradigmType != null) {
            bytes = paradigmType.toBytes(o, false);
        } else {
            bytes = SerializeUtil.serialize(o);
        }
        return bytes;
    }

    protected Object createObjectValue(DataType paradigmType, byte[] bytes, AtomicInteger offset) {
        Object value = null;
        if (paradigmType != null) {
            value = paradigmType.byteToValue(bytes, offset);
        } else {
            value = SerializeUtil.deserialize(bytes, offset);
        }
        return value;
    }

    protected String createStringValue(DataType paradigmType, Object object) {
        return createStringValue(paradigmType, object, null);
    }

    protected String createStringValue(DataType paradigmType, Object object, Class arrayElementClass) {
        String data = null;
        if (paradigmType == null) {
            JSONObject jsonObject = new JSONObject();
            DataType dataType = DataTypeUtil.getDataTypeFromClass(object.getClass());
            jsonObject.put(DATATYPE_NAME, dataType.getDataTypeName());
            jsonObject.put(ELEMENT_VALUE, dataType.toDataJson(object));
            if (arrayElementClass != null) {
                jsonObject.put(ELEMENT_CLASS_NAME, arrayElementClass.getName());
            }
            data = jsonObject.toJSONString();
        } else {
            data = paradigmType.toDataJson(object);
        }
        return data;
    }

    /**
     * 把java.util.List<java.lang.String>中的java.util.List去掉
     *
     * @param paradigmType
     * @return
     */
    protected String getParadigmTypeValue(String paradigmType) {
        if (StringUtil.isEmpty(paradigmType)) {
            return null;
        }
        if (paradigmType.endsWith("[]")) {// 数组
            paradigmType = paradigmType.substring(0, paradigmType.length() - 2);
            return paradigmType;
        }
        int index = paradigmType.indexOf("<");
        if (index == -1) {
            return paradigmType;
        }
        paradigmType = paradigmType.substring(index + 1, paradigmType.length() - 1);
        return paradigmType.trim();
    }

    public String getGenericParameterStr() {
        return genericParameterStr;
    }

    public void setGenericParameterStr(String genericParameterStr) {
        this.genericParameterStr = genericParameterStr;
    }

    protected abstract String createGenericParameterStr();
}
