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
package org.apache.rocketmq.streams.script.function.impl.type;

import java.io.UnsupportedEncodingException;

import org.apache.rocketmq.streams.script.context.FunctionContext;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.FunctionMethod;
import org.apache.rocketmq.streams.script.annotation.FunctionParamter;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

@Function
public class CastFunction {
    //
    @Deprecated
    @FunctionMethod(value = "cast", comment = "转换字符串为某种类型")
    public Object cast(IMessage message, FunctionContext context, @FunctionParamter("代表带转换数据的字段名或常量") String fieldName,
                       @FunctionParamter(value = "string", comment = "转换后的类型，支持字读名或常量，小写的类型如：string，int,long") String dataTypeName) {
        String value = FunctionUtils.getValueString(message, context, fieldName);
        if (value == null) {
            return null;
        }
        if (FunctionUtils.isConstant(dataTypeName)) {
            dataTypeName = FunctionUtils.getConstant(dataTypeName);
        }
        DataType dataType = DataTypeUtil.getDataType(dataTypeName);
        Object object = dataType.getData(value);
        return object;
    }

    @Deprecated
    @FunctionMethod(value = "byte2String", comment = "转换字符串为某种类型")
    public String byteEncode(IMessage message, FunctionContext context,
                             @FunctionParamter("代表带转换数据的字段名或常量") String fieldName,
                             @FunctionParamter(value = "string", comment = "转换后的类型，支持字读名或常量，小写的类型如：string，int,long") String chartcode) {
        Object value = FunctionUtils.getValue(message, context, fieldName);
        if (FunctionUtils.isConstant(chartcode)) {
            chartcode = FunctionUtils.getConstant(chartcode);
        }

        if (value == null || !byte[].class.isInstance(value)) {
            return null;
        }
        byte[] data = (byte[])value;
        String result = null;
        try {
            result = new String(data, chartcode);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return null;
        }
        return result;
    }

}
