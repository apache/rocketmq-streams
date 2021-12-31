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
import java.io.Serializable;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.SerializeUtil;

public class SerializableDataType extends BaseDataType {

    @Override public DataType create() {
        return this;
    }

    @Override public String getDataTypeName() {
        return "serializable";
    }

    @Override public boolean matchClass(Class clazz) {

        return Serializable.class.isAssignableFrom(clazz);
    }

    @Override public String toDataJson(Object value) {
        byte[] bytes = SerializeUtil.serializeByJava(value);
        return Base64Utils.encode(bytes);
    }

    @Override public Object getData(String jsonValue) {
        byte[] bytes = Base64Utils.decode(jsonValue);
        return SerializeUtil.deserializeByJava(bytes);
    }

    @Override public byte[] toBytes(Object value, boolean isCompress) {
        if (value == null) {
            return null;
        }

        byte[] result = SerializeUtil.serializeByJava(value);

        byte[] bytes = new byte[result.length + 2];
        byte[] lenBytes = createByteArrayFromNumber(result.length, 2);
        bytes[0] = lenBytes[0];
        bytes[1] = lenBytes[1];
        for (int i = 2; i < bytes.length; i++) {
            bytes[i] = result[i - 2];
        }
        return bytes;
    }

    @Override public Object byteToValue(byte[] bytes) {
        byte[] valueBytes = new byte[bytes.length - 2];
        for (int i = 0; i < valueBytes.length; i++) {
            valueBytes[i] = bytes[i + 2];
        }
        return SerializeUtil.deserializeByJava(valueBytes);
    }

    @Override protected byte[] numberToBytes(Long number) {
        return super.numberToBytes(number);
    }

    @Override protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

}
