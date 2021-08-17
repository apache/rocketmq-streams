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
import java.math.BigDecimal;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class DoubleDataType extends BaseDataType<Double> {
    /**
     *
     */
    private static final long serialVersionUID = 627430731714839798L;

    public DoubleDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public DoubleDataType() {
        setDataClazz(Double.class);
    }

    @Override
    public Double getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        return Double.valueOf(jsonValue);
    }

    @Override
    public String getName() {
        return double.class.getSimpleName();
    }

    public static String getTypeName() {
        return "double";
    }

    public static void main(String[] args) {
        System.out.println(double.class.getSimpleName());
    }

    @Override
    public String toDataJson(Double value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public double convertValue(Double value) {
        if (value == null) {
            return 0.0;
        }
        return value.doubleValue();
    }

    @Override
    public boolean matchClass(Class clazz) {
        if (BigDecimal.class.isAssignableFrom(clazz)) {
            return true;
        }
        return Double.class.isAssignableFrom(clazz) || double.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {BigDecimal.class, Double.class, double.class};
    }

    @Override
    public Double convert(Object object) {
        if (object == null) {
            return null;
        }
        if (BigDecimal.class.isInstance(object)) {
            return Double.valueOf(object.toString());
        }
        return super.convert(object);
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

    public String toDataJson(BigDecimal bigInteger) {
        Double value = convert(bigInteger);
        return toDataJson(value);
    }

    @Override
    public byte[] toBytes(Double value, boolean isCompress) {
        long lvalue = Double.doubleToRawLongBits(value);
        if (isCompress) {
            return numberToBytes(lvalue);
        } else {
            return createByteArrayFromNumber(lvalue, 8);
        }

    }

    @Override
    public Double byteToValue(byte[] bytes) {
        Long lvalue = createNumberValue(bytes);
        if (lvalue == null) {
            return null;
        }
        return Double.longBitsToDouble(lvalue);
    }

    @Override
    public Double byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 8);
        offset.set(index + 8);
        return byteToValue(bytesArray);
    }

    @Override
    public Class getDataClazz() {
        return Double.class;
    }
}
