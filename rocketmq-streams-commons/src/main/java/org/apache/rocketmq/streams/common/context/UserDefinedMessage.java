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
package org.apache.rocketmq.streams.common.context;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.Base64Utils;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.InstantiationUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

public class UserDefinedMessage extends JSONObject implements Serializable {

    protected Object messageValue;
    protected Map<String, Object> fieldMap;//如果messageValue是pojo，这里存储字段名和field信息
    protected boolean isList = false;
    protected boolean isBasic = false;
    protected boolean isMap = false;
    protected boolean isPojo = false;
    protected List<String> columnNames;//schama的列名，主要针对list场景

    public UserDefinedMessage(Object messageValue) {
        this(messageValue, null);
    }

    public UserDefinedMessage() {}

    public UserDefinedMessage(Object messageValue, List<String> columnNames) {
        this.messageValue = messageValue;
        this.columnNames = columnNames;

        this.fieldMap = new HashMap<>();
        Class<?> clazz = messageValue.getClass();
        if (DataTypeUtil.isNumber(clazz) || DataTypeUtil.isString(clazz) || DataTypeUtil.isBoolean(clazz)) {
            isBasic = true;
            if (columnNames != null && columnNames.size() > 0) {
                this.fieldMap.put(columnNames.get(0), messageValue);
            }
        } else if (DataTypeUtil.isList(clazz) || DataTypeUtil.isArray(clazz)) {
            isList = true;
            Iterator<?> it = ((Iterable<?>)messageValue).iterator();
            int i = 0;
            while (it.hasNext()) {
                Object value = it.next();
                String key = i + "";
                if (columnNames != null && columnNames.size() > 0) {
                    key = columnNames.get(i);
                }
                this.fieldMap.put(key, value);
                i++;
            }
        } else if (DataTypeUtil.isMap(clazz) || messageValue instanceof JSONObject) {
            isMap = true;
            this.fieldMap = (Map<String, Object>)messageValue;
        } else {
            isPojo = true;
            ReflectUtil.scanFields(messageValue, (o, field) -> this.fieldMap.put(field.getName(), field));
        }
    }

    public UserDefinedMessage(JSONObject jsonObject, String userObjectString) {
        this.putAll(jsonObject);
        this.messageValue = deserializeMessageValue(userObjectString);
    }

    @Override
    public int size() {
        return super.size() + fieldMap.size();
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean containsKey(Object key) {
        if (fieldMap.containsKey(key)) {
            return true;
        }
        return super.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        boolean isMatch = super.containsValue(value);
        if (isMatch) {
            return true;
        }
        for (String fieldName : fieldMap.keySet()) {
            if (messageValue != null) {
                Object msgValue = getBeanFieldOrJsonValue(messageValue, fieldName);
                if (msgValue == null) {
                    continue;
                }
                if (msgValue.equals(value)) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Object get(Object key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, (String)key);
        if (msgValue != null) {
            return msgValue;
        }
        return super.get(key);
    }

    @Override
    public JSONObject getJSONObject(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue instanceof JSONObject) {
            return (JSONObject)msgValue;
        }
        return super.getJSONObject(key);
    }

    @Override
    public JSONArray getJSONArray(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue instanceof JSONArray) {
            return (JSONArray)msgValue;
        }
        return super.getJSONArray(key);
    }

    @Override
    public <T> T getObject(String key, Class<T> clazz) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (clazz.isInstance(msgValue)) {
            return (T)msgValue;
        }
        return super.getObject(key, clazz);
    }

    @Override
    public Boolean getBoolean(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isBoolean(msgValue.getClass())) {
            return (Boolean)msgValue;
        }
        return super.getBoolean(key);
    }

    @Override
    public byte[] getBytes(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        return super.getBytes(key);
    }

    @Override
    public boolean getBooleanValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isBoolean(msgValue.getClass())) {
            return (Boolean)msgValue;
        }
        return super.getBooleanValue(key);
    }

    @Override
    public Byte getByte(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isByte(msgValue.getClass())) {
            return (Byte)msgValue;
        }
        return super.getByte(key);
    }

    @Override
    public byte getByteValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isByte(msgValue.getClass())) {
            return (Byte)msgValue;
        }
        return super.getByteValue(key);
    }

    @Override
    public Short getShort(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isShort(msgValue.getClass())) {
            return (Short)msgValue;
        }
        return super.getShort(key);
    }

    @Override
    public short getShortValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isShort(msgValue.getClass())) {
            return (Short)msgValue;
        }
        return super.getShortValue(key);
    }

    @Override
    public Integer getInteger(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isInt(msgValue.getClass())) {
            return (Integer)msgValue;
        }
        return super.getInteger(key);
    }

    @Override
    public int getIntValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isInt(msgValue.getClass())) {
            return (int)msgValue;
        }
        return super.getIntValue(key);
    }

    @Override
    public Long getLong(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isLong(msgValue.getClass())) {
            return (Long)msgValue;
        }
        return super.getLong(key);
    }

    @Override
    public long getLongValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isLong(msgValue.getClass())) {
            return (long)msgValue;
        }
        return super.getLongValue(key);
    }

    @Override
    public Float getFloat(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isFloat(msgValue.getClass())) {
            return (Float)msgValue;
        }
        return super.getFloat(key);
    }

    @Override
    public float getFloatValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isFloat(msgValue.getClass())) {
            return (float)msgValue;
        }
        return super.getFloatValue(key);
    }

    @Override
    public Double getDouble(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isDouble(msgValue.getClass())) {
            return (Double)msgValue;
        }
        return super.getDouble(key);
    }

    @Override
    public double getDoubleValue(String key) {
        Object msgValue = getBeanFieldOrJsonValue(messageValue, key);
        if (msgValue != null && DataTypeUtil.isDouble(msgValue.getClass())) {
            return (double)msgValue;
        }
        return super.getDoubleValue(key);
    }

    @Override
    public BigDecimal getBigDecimal(String key) {
        return super.getBigDecimal(key);
    }

    @Override
    public BigInteger getBigInteger(String key) {
        return super.getBigInteger(key);
    }

    @Override
    public String getString(String key) {
        return super.getString(key);
    }

    @Override
    public Date getDate(String key) {
        return super.getDate(key);
    }

    @Override
    public Object getSqlDate(String key) {
        return super.getSqlDate(key);
    }

    @Override
    public Object getTimestamp(String key) {
        return super.getTimestamp(key);
    }

    @Override
    public Object put(String key, Object value) {
        if (fieldMap.containsKey(key)) {
            ReflectUtil.setFieldValue(messageValue, key, value);
            return value;
        }
        return super.put(key, value);
    }

    @Override
    public void putAll(Map<? extends String, ?> m) {
        Iterator<? extends Entry<? extends String, ?>> it = m.entrySet().iterator();
        while (it.hasNext()) {
            Entry<? extends String, ?> entry = it.next();
            put(entry.getKey(), entry.getValue());
        }
    }

    @Override
    public Set<String> keySet() {
        Set<String> set = super.keySet();
        set.addAll(fieldMap.keySet());
        return set;
    }

    @Override
    public Collection<Object> values() {
        Collection<Object> values = super.values();
        Set<Object> result = new HashSet<>();
        result.addAll(values);
        for (String fieldName : fieldMap.keySet()) {
            Object value = getBeanFieldOrJsonValue(messageValue, fieldName);
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public Set<Entry<String, Object>> entrySet() {
        Set<Entry<String, Object>> set = super.entrySet();

        for (String fieldName : fieldMap.keySet()) {
            Object value = getBeanFieldOrJsonValue(messageValue, fieldName);
            if (value != null) {
                Entry<String, Object> entry = new Entry<String, Object>() {

                    @Override
                    public String getKey() {
                        return fieldName;
                    }

                    @Override
                    public Object getValue() {
                        return value;
                    }

                    @Override
                    public Object setValue(Object value) {
                        return null;
                    }
                };
                set.add(entry);
            }
        }
        return set;
    }

    protected Object getBeanFieldOrJsonValue(Object messageValue, String fieldName) {
        if (isList) {
            return fieldMap.get(fieldName);
        }
        if (isBasic) {
            return messageValue;
        }
        return ReflectUtil.getBeanFieldOrJsonValue(messageValue, fieldName);
    }

    @Override
    public String toString() {
        return messageValue.toString();
    }

    @Override
    public String toJSONString() {
        JSONObject jsonObject = new JSONObject();
        Set<String> set = super.keySet();
        for (String key : set) {
            jsonObject.put(key, super.get(key));
        }

        jsonObject.put(this.getClass().getName(), serializeMessageValue());
        return jsonObject.toJSONString();
    }

    protected String serializeMessageValue() {
        JSONObject msg = new JSONObject();
        String type = null;
        String data = null;
        if (isBasic || isList || (isMap && !(messageValue instanceof JSONObject))) {
            DataType dataType = DataTypeUtil.getDataTypeFromClass(messageValue.getClass());
            data = dataType.toDataJson(messageValue);
            type = "datatype";
        } else if (isMap) {
            JSONObject jsonObject = (JSONObject)messageValue;
            data = jsonObject.toJSONString();
            type = "json";
        } else {
            byte[] bytes = InstantiationUtil.serializeObject(messageValue);
            data = Base64Utils.encode(bytes);
            type = "java";
        }

        msg.put(IMessage.DATA_KEY, data);
        msg.put("type", type);
        return msg.toJSONString();
    }

    protected Object deserializeMessageValue(String userObjectString) {
        JSONObject msg = JSONObject.parseObject(userObjectString);
        String data = msg.getString(IMessage.DATA_KEY);
        String type = msg.getString("type");
        if ("datatype".equals(type)) {
            DataType dataType = DataTypeUtil.getDataTypeFromClass(messageValue.getClass());
            return dataType.getData(data);
        } else if ("json".equals(type)) {
            return JSONObject.parseObject(data);
        } else {
            byte[] bytes = Base64Utils.decode(data);
            return InstantiationUtil.deserializeObject(bytes);
        }

    }

    @Override
    public Object putIfAbsent(String key, Object value) {
        if (fieldMap.containsKey(key)) {
            ReflectUtil.setFieldValue(messageValue, key, value);
            return value;
        }
        return null;
    }

    public Object getMessageValue() {
        return messageValue;
    }

    public void setMessageValue(Object messageValue) {
        this.messageValue = messageValue;
    }

}
