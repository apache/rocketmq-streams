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
package org.apache.rocketmq.streams.common.configurable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.configurable.annotation.NoSerialized;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.model.Entity;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;

/**
 * 这个类自动完成成员变量的序列化，反序列化，以及环境变量的替换 子类只要按pojo实现即可。 有几个要求： 1.需要序列化的类，必须实现getset方法，这块下个版本会优化，去掉这个限制 2.不需要序列化的字段必须加transient 关键字声明 3.成员变量是 DataType支持的类型
 */
public class BasedConfigurable extends AbstractConfigurable implements IConfigurableIdentification {

    /**
     * 扩展字段
     */
    public static final String EXTEND_FIELD_NAME = "extendField";
    protected transient List<ENVField> envVarRegister = new ArrayList<>();

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        setJsonObject(jsonObject);
        jsonObject.put(CLASS_NAME, this.getClass().getName());
        return jsonObject;
    }

    @Override public String toJson() {
        JSONObject jsonObject = toJsonObject();
        return jsonObject.toJSONString();
    }

    protected void setJsonObject(JSONObject jsonObject) {
        Class<? extends BasedConfigurable> thisClass = this.getClass();
        JSONObject tmp = jsonObject.getJSONObject(EXTEND_FIELD_NAME);
        setJsonObject(thisClass, jsonObject);
    }

    protected void setJsonObject(Class<?> clazz, JSONObject jsonObject) {
        if (clazz == null) {
            return;
        }
        if (Entity.class.getName().equals(clazz.getName())) {
            return;
        }
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            if (field.isAnnotationPresent(NoSerialized.class)) {
                continue;
            }
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            } else if (Modifier.isTransient(field.getModifiers())) {
                continue;
            } else if (Modifier.isNative(field.getModifiers())) {
                continue;
            } else if (field.getName().endsWith("this$0")) {
                continue;
            } else if (field.isAnnotationPresent(ConfigurableReference.class)) {

                IConfigurable configurable = ReflectUtil.getBeanFieldValue(this, field.getName());
                if (configurable != null) {
                    jsonObject.put(field.getName() + ".type", configurable.getType());
                    jsonObject.put(field.getName(), configurable.getName());
                }
                continue;

            }
            DataType dataType = null;
            try {
                dataType = DataTypeUtil.createFieldDataType(this, field.getName());
                Object fieldValue = ReflectUtil.getBeanFieldValue(this, field.getName());
                if (fieldValue != null) {
                    // 如果是空值则不再处理。入库也没意义
                    String fieldValueStr = dataType.toDataJson(fieldValue);
                    jsonObject.put(field.getName(), fieldValueStr);
                }
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("序列化字段 " + getClass().getName() + "." + field.getName() + "error", e);
            }

        }
        Class<?> parent = clazz.getSuperclass();
        setJsonObject(parent, jsonObject);
    }

    public int getStatus() {
        return 1;
    }

    protected void getJsonObject(JSONObject jsonObject) {
        Class<?> thisClass = this.getClass();
        getJsonObject(thisClass, jsonObject);
    }

    protected void getJsonObject(Class<?> clazz, JSONObject jsonObject) {
        if (Entity.class.getName().equals(clazz.getName())) {
            return;
        }
        if (Object.class.getName().equals(clazz.getName())) {
            return;
        }
        Field[] fields = clazz.getDeclaredFields();
        for (Field field : fields) {
            DataType<?> dataType = null;
            Object fieldValue = null;
            if (field.isAnnotationPresent(NoSerialized.class)) {
                continue;
            }
            if (Modifier.isStatic(field.getModifiers())) {
                continue;
            } else if (Modifier.isTransient(field.getModifiers())) {
                continue;
            } else if (Modifier.isNative(field.getModifiers())) {
                continue;
            } else if (field.isAnnotationPresent(ConfigurableReference.class)) {

                String configName = jsonObject.getString(field.getName());
                String type = jsonObject.getString(field.getName() + ".type");
                this.envVarRegister.add(new ENVField(type, configName, field.getName()));
                continue;

            } else {
                dataType = DataTypeUtil.createFieldDataType(this, field.getName());
                String fieldJsonStr = jsonObject.getString(field.getName());
                try {
                    fieldValue = dataType.getData(fieldJsonStr);
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

            if (fieldValue != null) {
                ReflectUtil.setBeanFieldValue(this, field.getName(), fieldValue);
            } else {
                ReflectUtil.setFieldValue(this, field.getName(), null);
            }
        }
        Class<?> parent = clazz.getSuperclass();
        getJsonObject(parent, jsonObject);
    }

    @Override public void toObject(String jsonString) {
        JSONObject jsonObject = JSON.parseObject(jsonString);
        getJsonObject(jsonObject);
    }

    public List<ENVField> getEnvVarRegister() {
        return envVarRegister;
    }

}
