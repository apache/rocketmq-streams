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
import com.alibaba.fastjson.JSONArray;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.ContantsUtil;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class SetDataType extends GenericParameterDataType<Set> {

    private static final long serialVersionUID = -2590322335704835947L;

    private transient DataType paradigmType;

    public SetDataType(Class clazz, DataType paradigmType) {
        setDataClazz(clazz);
        this.paradigmType = paradigmType;
        this.setGenericParameterStr(createGenericParameterStr());
    }

    public SetDataType(DataType paradigmType) {
        setDataClazz(Set.class);
        this.paradigmType = paradigmType;
        this.setGenericParameterStr(createGenericParameterStr());
    }

    public SetDataType() {
        setDataClazz(Set.class);

    }

    public static String getTypeName() {
        return "set";
    }

    @Override
    public String toDataStr(Set value) {
        StringBuilder stringBuilder = new StringBuilder();
        boolean isFirst = true;
        if (CollectionUtil.isNotEmpty(value)) {
            for (Object object : value) {
                if (object == null) {
                    continue;
                }
                DataType dataType = this.paradigmType;
                if (dataType == null) {
                    dataType = new StringDataType();
                }
                String str = dataType.toDataJson(object.toString());
                if (isFirst) {
                    isFirst = false;
                } else {
                    stringBuilder.append(",");
                }
                if (str.indexOf(",") != -1) {
                    stringBuilder.append("'" + str + "'");
                } else {
                    stringBuilder.append(str);
                }

            }
        }
        return stringBuilder.toString();
    }

    @Override
    public String toDataJson(Set value) {
        if (JSONArray.class.isInstance(value)) {
            return ((JSONArray) value).toJSONString();
        }
        JSONArray jsonArray = new JSONArray();
        if (CollectionUtil.isNotEmpty(value)) {
            for (Object object : value) {
                if (object == null) {
                    continue;
                }
                String data = createStringValue(this.paradigmType, object);
                jsonArray.add(data);
            }
        }
        return jsonArray.toJSONString();
    }

    @Override
    public void setDataClazz(Class dataClazz) {
        this.dataClazz = Set.class;
    }

    @Override
    public Set getData(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return null;
        }
        DataType dataType = this.paradigmType;
        if (isQuickModel(jsonValue)) {
            jsonValue = createJsonValue(jsonValue);
            if (dataType == null) {
                dataType = new StringDataType();
            }
        }
        JSONArray jsonArray = JSON.parseArray(jsonValue);
        Set list = new HashSet();
        for (int i = 0; i < jsonArray.size(); i++) {
            String json = jsonArray.getString(i);
            Object result = createObjectValue(dataType, json);
            list.add(result);
        }
        return list;
    }

    @Override
    public byte[] toBytes(Set value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        Iterator it = value.iterator();
        List<byte[]> list = new ArrayList<>();
        int len = 0;
        while (it.hasNext()) {
            Object o = it.next();
            if (o == null) {
                continue;
            }
            byte[] bytes = createByteValue(this.paradigmType, o);

            list.add(bytes);
            len = len + bytes.length;
        }
        byte[] bytes = new byte[len + 2];
        byte[] lenBytes = createByteArrayFromNumber(value.size(), 2);
        bytes[0] = lenBytes[0];
        bytes[1] = lenBytes[1];
        int i = 0;
        for (byte[] bytes1 : list) {
            for (byte b : bytes1) {
                bytes[i + 2] = b;
                i++;
            }
        }
        return bytes;
    }

    @Override
    public Set byteToValue(byte[] bytes) {
        return byteToValue(bytes, 0);
    }

    @Override
    public Set byteToValue(byte[] bytes, AtomicInteger offset) {
        if (bytes == null) {
            return null;
        }
        int len = createNumberValue(bytes, offset.get(), 2).intValue();
        offset.addAndGet(2);
        Set set = new HashSet();
        for (int i = 0; i < len; i++) {
            Object value = createObjectValue(paradigmType, bytes, offset);
            set.add(value);

        }
        return set;
    }

    private String createJsonValue(String jsonValue) {
        String value = jsonValue;
        Map<String, String> flag2ExpressionStr = new HashMap<>();
        boolean containsContant = ContantsUtil.containContant(jsonValue);
        if (containsContant) {
            value = ContantsUtil.doConstantReplace(jsonValue, flag2ExpressionStr, 1);
        }
        JSONArray jsonArray = new JSONArray();
        String[] values = value.split(",");
        for (int i = 0; i < values.length; i++) {
            String tmp = values[i];
            if (containsContant) {
                tmp = ContantsUtil.restore(tmp, flag2ExpressionStr);
                if (ContantsUtil.isContant(tmp)) {
                    tmp = tmp.substring(1, tmp.length() - 1);
                }

            }
            jsonArray.add(tmp);
        }
        return jsonArray.toJSONString();
    }

    protected boolean isQuickModel(String jsonValue) {
        if (StringUtil.isEmpty(jsonValue)) {
            return false;
        }
        if (jsonValue.trim().startsWith("{") == false && jsonValue.trim().startsWith("[") == false) {
            return true;
        }
        return false;
    }

    @Override
    public boolean matchClass(Class clazz) {
        return Set.class.isAssignableFrom(clazz);
    }

    @Override
    public DataType create() {
        return new SetDataType();
    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    public List convertValue(ArrayList value) {
        if (value == null) {
            return null;
        }
        return (List) value;
    }

    @Override
    public void parseGenericParameter(String genericParameterString) {
        if (StringUtil.isEmpty(genericParameterString)) {
            return;
        }
        genericParameterString = genericParameterString.trim();
        int index = Set.class.getName().length() + 1;
        String subClassString = genericParameterString.substring(index, genericParameterString.length() - 1);
        index = subClassString.indexOf("<");
        if (index != -1) {
            String className = subClassString.substring(0, index);
            Class clazz = createClass(className);
            DataType dataType = DataTypeUtil.getDataTypeFromClass(clazz);
            if (GenericParameterDataType.class.isInstance(dataType)) {
                GenericParameterDataType tmp = (GenericParameterDataType) dataType;
                tmp.parseGenericParameter(subClassString);
            }

            this.paradigmType = dataType;
        } else {
            Class clazz = createClass(subClassString);
            this.paradigmType = DataTypeUtil.getDataTypeFromClass(clazz);
        }
    }

    @Override
    protected String createGenericParameterStr() {
        String subStr = createGenericParameterStr(paradigmType);
        return List.class.getName() + "<" + subStr + ">";
    }

    public DataType getParadigmType() {
        return paradigmType;
    }

    public void setParadigmType(DataType paradigmType) {
        this.paradigmType = paradigmType;
    }
}
