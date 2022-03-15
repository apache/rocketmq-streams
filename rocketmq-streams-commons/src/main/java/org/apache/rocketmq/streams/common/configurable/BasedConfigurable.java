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
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.configurable.annotation.NoSerialized;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ENVUtile;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 这个类自动完成成员变量的序列化，反序列化，以及环境变量的替换 子类只要按pojo实现即可。 有几个要求： 1.需要序列化的类，必须实现getset方法，这块下个版本会优化，去掉这个限制 2.不需要序列化的字段必须加transient 关键字声明 3.成员变量是 DataType支持的类型
 */
public class BasedConfigurable extends AbstractConfigurable {

    /**
     * 扩展字段
     */
    public static final String EXTEND_FIELD_NAME = "extendField";

    private String nameSpace;

    private String configureName;

    private String type;

    protected String version = "1.0";

    protected long updateFlag = 0;//通过它来触发更新，其他字段变更都不会触发更新

    @Override
    public String getNameSpace() {
        return nameSpace;
    }

    @Override
    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }

    @Override
    public String getConfigureName() {
        return configureName;
    }

    @Override
    public void setConfigureName(String configureName) {
        this.configureName = configureName;
    }

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void setType(String type) {
        this.type = type;
    }

    public JSONObject toJsonObject() {
        JSONObject jsonObject = new JSONObject();
        setJsonObject(jsonObject);
        jsonObject.put(IConfigurableService.CLASS_NAME, this.getClass().getName());
        return jsonObject;
    }

    @Override
    public String toJson() {
        JSONObject jsonObject = toJsonObject();
        return jsonObject.toJSONString();
    }

    protected void setJsonObject(JSONObject jsonObject) {
        Class<? extends BasedConfigurable> thisClass = this.getClass();
        JSONObject tmp = jsonObject.getJSONObject(EXTEND_FIELD_NAME);
        setJsonObject(thisClass, jsonObject);
    }

    protected void setJsonObject(Class clazz, JSONObject jsonObject) {
        if (AbstractConfigurable.class.getName().equals(clazz.getName())) {
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
            }

            DataType dataType = DataTypeUtil.createFieldDataType(this, field.getName());
            Object fieldValue = ReflectUtil.getBeanFieldValue(this, field.getName());
            if (fieldValue != null) {
                // 如果是空值则不再处理。入库也没意义
                String fieldValueStr = dataType.toDataJson(fieldValue);
                fieldValueStr = restoreFieldValue(field, fieldValueStr);
                jsonObject.put(field.getName(), fieldValueStr);
            }
        }
        Class parent = clazz.getSuperclass();
        setJsonObject(parent, jsonObject);
    }

    public int getStatus() {
        int status = 1;
        if (this.getPrivateData("status") != null) {
            status = Integer.valueOf(this.getPrivateData("status"));
        }
        return status;
    }

    protected void getJsonObject(JSONObject jsonObject) {
        Class thisClass = this.getClass();
        getJsonObject(thisClass, jsonObject);
    }

    protected void getJsonObject(Class clazz, JSONObject jsonObject) {
        if (AbstractConfigurable.class.getName().equals(clazz.getName())) {
            return;
        }
        if (Object.class.getName().equals(clazz.getName())) {
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
            }
            DataType dataType = DataTypeUtil.createFieldDataType(this, field.getName());
            String fieldJsonStr = jsonObject.getString(field.getName());
            fieldJsonStr = getENVParamter(field, fieldJsonStr);
            Object fieldValue = dataType.getData(fieldJsonStr);
            if (fieldValue != null) {
                ReflectUtil.setBeanFieldValue(this, field.getName(), fieldValue);
            } else {
                ReflectUtil.setFieldValue(this, field.getName(), null);
            }
        }
        Class parent = clazz.getSuperclass();
        getJsonObject(parent, jsonObject);
    }

    /**
     * 支持存储env的key值，而具体的值存储在IENVParameter参数中
     *
     * @param fieldValue
     * @return
     */
    protected String getENVParamter(Field field, String fieldValue) {
        ENVDependence dependence = field.getAnnotation(ENVDependence.class);
        if (dependence == null) {
            return fieldValue;
        }
        String value = getENVVar(fieldValue);

        if (StringUtil.isNotEmpty(value)) {
            String key = MapKeyUtil.createKey(getNameSpace(), getType(), getConfigureName(), field.getName(), value);
            this.putPrivateData(key, fieldValue);
            return value;
        }
        return fieldValue;

    }

    protected String getENVVar(String fieldValue) {
        if (StringUtil.isEmpty(fieldValue)) {
            return null;
        }
        String value = ComponentCreator.getProperties().getProperty(fieldValue);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return ENVUtile.getENVParameter(fieldValue);
    }

    private String restoreFieldValue(Field field, String fieldValueStr) {
        return getOriFieldValue(field, fieldValueStr);
    }

    public String getOriFieldValue(Field field, String fieldValueStr) {
        ENVDependence dependence = field.getAnnotation(ENVDependence.class);
        if (dependence == null) {
            return fieldValueStr;
        }
        String key =
            MapKeyUtil.createKey(getNameSpace(), getType(), getConfigureName(), field.getName(), fieldValueStr);
        String oriFieldValue = this.getPrivateData(key);
        //        if(needRemove){
        //            this.removePrivateData(key);
        //        }

        if (StringUtil.isNotEmpty(oriFieldValue)) {
            return oriFieldValue;
        }
        return fieldValueStr;
    }

    @Override
    public void toObject(String jsonString) {
        JSONObject jsonObject = JSON.parseObject(jsonString);
        getJsonObject(jsonObject);
    }

    //    @Override
    //    public String toString() {
    //        return toJson();
    //    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public long getUpdateFlag() {
        return updateFlag;
    }

    public void setUpdateFlag(long updateFlag) {
        this.updateFlag = updateFlag;
    }
}
