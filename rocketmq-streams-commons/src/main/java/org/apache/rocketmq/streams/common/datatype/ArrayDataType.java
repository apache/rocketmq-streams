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
import java.util.List;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class ArrayDataType<T> extends GenericParameterDataType<T[]> {

    private static final long serialVersionUID = 1679897087225371547L;
    private DataType paradigmType;
    private transient ListDataType listDataType;

    public ArrayDataType(Class clazz, DataType paradigmType) {
        setDataClazz(clazz);
        this.paradigmType = paradigmType;
        listDataType = new ListDataType(paradigmType);
    }

    public ArrayDataType() {

    }

    @Override
    public String toDataJson(T[] value) {
        JSONArray jsonArray = new JSONArray();
        for (Object object : value) {
            jsonArray.add(paradigmType.toDataJson(object));
        }
        return jsonArray.toJSONString();
    }

    @Override
    public T[] getData(String jsonValue) {
        JSONArray jsonArray = JSON.parseArray(jsonValue);
        List list = new ArrayList();
        for (int i = 0; i < jsonArray.size(); i++) {
            String json = jsonArray.getString(i);
            Object result = json;
            if (paradigmType != null) {
                result = paradigmType.getData(json);
            }
            list.add(result);
        }
        return (T[])list.toArray();
    }

    @Override
    public String getName() {
        return "array";
    }

    public static String getTypeName() {
        return "array";
    }

    @Override
    public String getDataTypeName() {
        return "array";
    }

    @Override
    public void parseGenericParameter(String genericParameterString) {
        if (StringUtil.isEmpty(genericParameterString)) {
            return;
        }
        genericParameterString = genericParameterString.trim();
        String className = null;
        if (genericParameterString.endsWith("[]")) {
            className = genericParameterString.substring(0, genericParameterString.length() - 2);
        } else {
            className = genericParameterString.substring(2);
        }
        Class clazz = createClass(className);
        DataType dataType = DataTypeUtil.getDataTypeFromClass(clazz);
        if (GenericParameterDataType.class.isInstance(dataType)) {
            GenericParameterDataType genericParamterDataType = (GenericParameterDataType)dataType;
            genericParamterDataType.parseGenericParameter(className);
            this.paradigmType = dataType;
            return;
        }
        this.paradigmType = dataType;
    }

    @Override
    public String toDataStr(T[] ts) {
        List<T> list = convert(ts);
        return listDataType.toDataStr(list);
    }

    @Override
    protected String createGenericParameterStr() {
        String subStr = createGenericParameterStr(paradigmType);
        return "[L" + subStr;
    }

    @Override
    public boolean matchClass(Class clazz) {
        return clazz.isArray();
    }

    @Override
    public DataType create() {
        return new ArrayDataType<>();
    }

    public static void main(String[] args) {
        String[] strs = new String[10];
        ArrayDataType arrayDataType = new ArrayDataType(strs.getClass(), new StringDataType());
        System.out.println(arrayDataType.createGenericParameterStr());
    }

    protected List<T> convert(T[] ts) {
        if (ts == null) {
            return null;
        }
        List<T> tList = new ArrayList<T>();
        for (T t : ts) {
            tList.add(t);
        }
        return tList;
    }

    public void setParadigmType(DataType paradigmType) {
        this.paradigmType = paradigmType;
    }
}
