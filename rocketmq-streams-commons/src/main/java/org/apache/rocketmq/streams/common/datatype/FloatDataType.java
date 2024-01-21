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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class FloatDataType extends BaseDataType<Float> {
    private static final long serialVersionUID = 644884726927395233L;

    public FloatDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public FloatDataType() {
        setDataClazz(Float.class);
    }

    public static String getTypeName() {
        return "float";
    }

    public static void main(String[] args) {
        FloatDataType fdt = new FloatDataType();
        float f1 = 3.2f;
        byte[] b1 = fdt.toBytes(f1, true);
        byte[] b2 = fdt.toBytes(f1, false);
        float f11 = fdt.byteToValue(b1);
        float f12 = fdt.byteToValue(b2);
        System.out.println(f11);
        System.out.println(f12);

        DoubleDataType dt = new DoubleDataType();
        double d1 = 3.2d;
        byte[] db1 = dt.toBytes(d1, true);
        byte[] db2 = dt.toBytes(d1, false);
        double d11 = dt.byteToValue(db1);
        double d12 = dt.byteToValue(db2);
        System.out.println(d11);
        System.out.println(d12);
    }

    @Override
    public Float getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        return Float.valueOf(jsonValue);
    }

    @Override
    public String getName() {
        return float.class.getSimpleName();
    }

    @Override
    public String toDataJson(Float value) {
        return String.valueOf(value);
    }

    public float convertValue(Float value) {
        if (value == null) {
            return 0;
        }
        return value.floatValue();
    }

    @Override
    public boolean matchClass(Class clazz) {
        return Float.class.isAssignableFrom(clazz) || float.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Float.class, float.class};
    }

    @Override
    public DataType create() {
        return this;
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
    public byte[] toBytes(Float value, boolean isCompress) {
        int lvalue = Float.floatToRawIntBits(value);
        if (isCompress) {
            return numberToBytes((long) lvalue);
        } else {
            return createByteArrayFromNumber(lvalue, 8);
        }

    }

    @Override
    public Float byteToValue(byte[] bytes) {
        Long lvalue = createNumberValue(bytes);
        if (lvalue == null) {
            return null;
        }
        return Float.intBitsToFloat(lvalue.intValue());
    }

    @Override
    public Float byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 8);
        offset.set(index + 8);
        return byteToValue(bytesArray);
    }
}
