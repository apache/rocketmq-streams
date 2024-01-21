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
import org.apache.rocketmq.streams.common.utils.StringUtil;

public abstract class GenericParamterDataType<T> extends BaseDataType<T> {

    protected String genericParamterStr;

    public abstract void parsegenericParameter(String genericParameterString);

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {
        if (genericParamterStr != null) {
            jsonObject.put(GENERIC_PARAMETER, genericParamterStr);
        }
    }

    public abstract String toDataStr(T t);

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {
        this.genericParamterStr = jsonObject.getString(GENERIC_PARAMETER);
        if (StringUtil.isNotEmpty(this.genericParamterStr)) {
            this.genericParamterStr = this.genericParamterStr.trim();
        }
        parsegenericParameter(this.genericParamterStr);
    }

    @Override
    protected Class[] getSupportClass() {
        return null;
    }

    //    protected DataType parseParadigmType(String genericParameterType) {
    //        if (genericParameterType == null) return null;
    //        String typeString = genericParameterType.trim();
    //        int index = typeString.indexOf("<");
    //        String className = genericParameterType;
    //        if (index > -1) {
    //            className = typeString.substring(0, index).trim();
    //            typeString = typeString.substring(index + 1, typeString.length() - 1).trim();
    //            DataType dataType=parseParadigmType(typeString);
    //        }else{
    //            Class clazz  = Class.forName(className);
    //            DataType dataType=createDataTypeFromClass(clazz);
    //            return dataType;
    //        }
    //
    //        Class clazz = null;
    //        try {
    //            clazz = Class.forName(className);
    //            DataType dataType = createDateTypeForGenericParameter(clazz, typeString, false);
    //            return dataType;
    //        } catch (ClassNotFoundException e) {
    //            e.printStackTrace();
    //        }
    //        return null;
    //    }

    protected String createGenericParamterStr(DataType paradigmType) {
        if (paradigmType == null) {
            return null;
        }
        String subStr = null;
        if (GenericParamterDataType.class.isInstance(paradigmType)) {
            GenericParamterDataType genericParamterDataType = (GenericParamterDataType) paradigmType;
            subStr = genericParamterDataType.createGenericParamterStr();
        } else {
            subStr = paradigmType.getDataClass().getName();
        }
        return subStr;
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

    public String getGenericParamterStr() {
        return genericParamterStr;
    }

    public void setGenericParamterStr(String genericParamterStr) {
        this.genericParamterStr = genericParamterStr;
    }

    protected abstract String createGenericParamterStr();
}
