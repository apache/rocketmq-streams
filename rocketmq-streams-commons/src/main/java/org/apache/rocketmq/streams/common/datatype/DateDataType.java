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
import java.sql.Timestamp;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.utils.NumberUtils;

public class DateDataType extends BaseDataType<Date> {

    private static final long serialVersionUID = 3745784172508184101L;
    private static final Log LOG = LogFactory.getLog(DateDataType.class);

    public DateDataType(Class clazz) {
        setDataClazz(clazz);
    }

    public DateDataType() {
        setDataClazz(Date.class);
    }

    @Override
    public String toDataJson(Date value) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String dateString = sdf.format(value);
        return dateString;
    }

    @Override
    public Date getData(String jsonValue) {
        if (jsonValue == null || "N/A".equals(jsonValue)) {
            return null;
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        try {
            ParsePosition pos = new ParsePosition(0);
            Date datetime = sdf.parse(jsonValue, pos);
            Timestamp ts = null;

            if (datetime != null) {

                ts = new Timestamp(datetime.getTime());
            }
            return ts;
        } catch (Exception e) {
            LOG.error("an not calculate expireTime,parse date error:" + jsonValue);
            return null;
        }
    }

    public static void main(String[] args) {
        new DateDataType().getData("2017-03-16 16:53:13");
    }

    @Override
    public Class getDataClazz() {
        return Date.class;
    }

    @Override
    public Date convert(Object object) {
        if (object == null) {
            return null;
        }
        if (Timestamp.class.isInstance(object)) {
            return new Date(((Timestamp) object).getTime());
        }
        if (object instanceof LocalDateTime) {
            LocalDateTime tempTime = (LocalDateTime) object;
            return Date.from(tempTime.atZone(ZoneId.systemDefault()).toInstant());
        }
        Date convert = (Date) super.convert(object);
        return convert;
    }

    public static String getTypeName() {
        return "date";
    }

    public String toDataJson(Timestamp timestamp) {
        Date value = convert(timestamp);
        return toDataJson(value);
    }

    @Override
    public boolean matchClass(Class clazz) {
        if (Timestamp.class.isAssignableFrom(clazz)) {
            return true;
        }
        return Date.class.isAssignableFrom(clazz);
    }

    @Override
    protected Class[] getSupportClass() {
        return new Class[] {Timestamp.class, Date.class};
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
    public byte[] toBytes(Date value, boolean isCompress) {
        if (value == null) {
            return null;
        }
        return createByteArrayFromNumber(value.getTime(), 8);
    }

    @Override
    public Date byteToValue(byte[] bytes) {
        Long value = createNumberValue(bytes);
        if (value == null) {
            return null;
        }
        return new Date(value);
    }

    @Override
    public Date byteToValue(byte[] bytes, int offset) {
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, offset, 8);
        return byteToValue(bytesArray);
    }

    @Override public Date byteToValue(byte[] bytes, AtomicInteger offset) {
        byte[] bytesArray = NumberUtils.getSubByteFromIndex(bytes, offset.get(), 8);
        offset.set(offset.get() + bytesArray.length);
        return byteToValue(bytesArray);
    }
}
