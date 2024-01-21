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
import java.math.BigInteger;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class LongDataType extends BaseDataType<Long> {

    private static final long serialVersionUID = -7361280645906940750L;

    private final static String DOUBLE = "^[-\\+]?[\\d]*[\\.]?[\\d]*$|[\\+\\-]?[\\d]+([\\.][\\d]*)?([Ee][+-]?[\\d]+)?";

    public LongDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public LongDataType() {
        setDataClazz(Long.class);
    }

    public static String getTypeName() {
        return "long";
    }

    @Override
    public Long getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }
        boolean match = StringUtil.matchRegex(jsonValue, DOUBLE);
        if (match) {
            Double value = Double.valueOf(jsonValue);
            return value.longValue();
        }
        try {
            return Long.valueOf(jsonValue);
        } catch (Exception e) {
            try {
                BigDecimal decimal = new BigDecimal(jsonValue);
                return decimal.longValue();
            } catch (Exception e1) {
                //TODO
                return null;
            }
        }
    }

    @Override
    public boolean matchClass(Class clazz) {
        if (BigInteger.class.isAssignableFrom(clazz)) {
            return true;
        }
        return Long.class.isAssignableFrom(clazz) || long.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {BigInteger.class, Long.class, long.class};
    }

    @Override
    public DataType create() {
        return this;
    }

    @Override
    protected void setFieldValueToJson(JSONObject jsonObject) {

    }

    @Override
    public String getDataTypeName() {
        return getTypeName();
    }

    @Override
    protected void setFieldValueFromJson(JSONObject jsonObject) {

    }

    @Override
    public Long convert(Object object) {
        if (object == null) {
            return null;
        }
        if (BigInteger.class.isInstance(object)) {
            return Long.valueOf(object.toString());
        }
        return super.convert(object);
    }

    @Override
    public String getName() {
        return Long.class.getSimpleName();
    }

    public String toDataJson(BigInteger bigInteger) {
        Long value = convert(bigInteger);
        return toDataJson(value);
    }

    @Override
    public Class getDataClazz() {
        return Long.class;
    }

    @Override
    public void setDataClazz(Class dataClazz) {
        if (BigInteger.class.isAssignableFrom(dataClazz)) {
            dataClazz = Long.class;
        }
        super.setDataClazz(dataClazz);
    }

    @Override
    public String toDataJson(Long value) {
        if (value == null) {
            return null;
        }
        return String.valueOf(value);
    }

    public long convertValue(Long value) {
        if (value == null) {
            return 0;
        }
        return value.longValue();
    }

    @Override
    public byte[] toBytes(Long value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        if (isCompress) {
            return numberToBytes((long) value);
        } else {
            return createByteArrayFromNumber(value, 8);
        }
    }

    @Override
    public Long byteToValue(byte[] bytes) {
        Long value = createNumberValue(bytes);
        return value;
    }

    @Override
    public Long byteToValue(byte[] bytes, AtomicInteger offset) {
        int index = offset.get();
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, index, 8);
        offset.set(index + 8);
        return byteToValue(bytesArray);
    }

}
