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
package org.apache.rocketmq.streams.common.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IFieldProcessor;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;

public class ConfigurableUtil {
    protected static class FieldProcessorGetter implements IFieldProcessor {
        private Map<String, String> fieldName2JsonValue = new HashMap<>();
        private Map<String, String> changeableFieldName2JsonValue = new HashMap<>();
        private JSONObject message;

        public FieldProcessorGetter(String message) {
            this.message = JSON.parseObject(message);
        }

        @Override
        public void doProcess(Object o, Field field) {
            String fieldJsonStr = message.getString(field.getName());
            if (field.getAnnotation(Changeable.class) != null) {//对标记易变的字段不参与比较
                changeableFieldName2JsonValue.put(field.getName(), fieldJsonStr);
            }
            fieldName2JsonValue.put(field.getName(), fieldJsonStr);
        }
    }

    public static IConfigurable create(String className, String namespace, String name, JSONObject property, JSONObject mock) {
        IConfigurable configurable = ConfigurableUtil.create(namespace, name, property, className);
        if (mock != null) {
            addMockData(mock, property);
        }
        return configurable;
    }

    public static void addMockData(JSONObject mock, JSONObject property) {
        for (Map.Entry<String, Object> entry : mock.entrySet()) {
            String fieldName = entry.getKey();
            String value = (String) property.get(fieldName);
            if (StringUtil.isNotEmpty(value)) {
                String actualValue = (String) entry.getValue();
                ComponentCreator.getProperties().put(value, actualValue);
            }
        }
    }

    public static boolean compare(IConfigurable configurable1, IConfigurable configurable2) {
        if (!configurable1.getNameSpace().equals(configurable2.getNameSpace())) {
            return false;
        }
        if (!configurable1.getType().equals(configurable2.getType())) {
            return false;
        }
        if (!configurable1.getConfigureName().equals(configurable2.getConfigureName())) {
            return false;
        }
        if (configurable1 instanceof BasedConfigurable && configurable2 instanceof BasedConfigurable) {
            BasedConfigurable abstractConfigurable1 = (BasedConfigurable) configurable1;
            BasedConfigurable abstractConfigurable2 = (BasedConfigurable) configurable2;
            if (abstractConfigurable1.getUpdateFlag() == abstractConfigurable2.getUpdateFlag()) {
                return true;
            } else {
                return false;
            }
        } else {
            FieldProcessorGetter fieldProcessorGetter1 = new FieldProcessorGetter(configurable1.toJson());
            FieldProcessorGetter fieldProcessorGetter2 = new FieldProcessorGetter(configurable2.toJson());
            ReflectUtil.scanConfiguableFields(configurable1, fieldProcessorGetter1);
            ReflectUtil.scanConfiguableFields(configurable2, fieldProcessorGetter2);
            Map<String, String> fieldName2JsonValue1 = fieldProcessorGetter1.fieldName2JsonValue;
            Map<String, String> fieldName2JsonValue2 = fieldProcessorGetter2.fieldName2JsonValue;
            if (fieldName2JsonValue1.size() != fieldName2JsonValue2.size()) {
                return false;
            }
            Iterator<Map.Entry<String, String>> it = fieldName2JsonValue1.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                String key = entry.getKey();
                if (fieldProcessorGetter1.changeableFieldName2JsonValue.containsKey(key)) {
                    continue;
                }
                String value = entry.getValue();
                String otherValue = fieldName2JsonValue2.get(key);
                if (otherValue == null && otherValue == null) {
                    continue;
                }
                if (otherValue == null) {
                    return false;
                }
                if (!otherValue.equals(value)) {
                    return false;
                }
            }
        }

        return true;
    }

    public static void refreshMock(IConfigurable configurable) {
        configurable.toObject(configurable.toJson());
    }

    public static <T extends BasedConfigurable> T create(String namespace, String name, String className) {
        IConfigurable configurable = ReflectUtil.forInstance(className);
        configurable.setNameSpace(namespace);
        configurable.setConfigureName(name);
        return (T) configurable;
    }

    public static <T extends BasedConfigurable> T create(String namespace, String name, JSONObject message,
        String className) {
        IConfigurable configurable = create(namespace, name, className);
        initProperty(configurable, message);
        return (T) configurable;
    }

    public static void initProperty(IConfigurable configurable, JSONObject message) {
        ReflectUtil.scanConfiguableFields(configurable, new IFieldProcessor() {
            @Override
            public void doProcess(Object o, Field field) {
                setProperty2Configurable(o, field.getName(), message);
            }
        });
    }

    public static void setProperty2Configurable(Object configurable, String filedName, JSONObject message) {
        Object field = message.get(filedName);
        if (field == null) {
            return;
        }
        Object fieldValue = field;
        if (String.class.isInstance(field)) {
            String fieldJsonStr = (String) field;
            DataType dataType = DataTypeUtil.createFieldDataType(configurable, filedName);
            fieldValue = dataType.getData(fieldJsonStr);
        }

        if (fieldValue != null) {
            ReflectUtil.setBeanFieldValue(configurable, filedName, fieldValue);
        }
    }

    /**
     * @param namespace
     * @param name
     * @param fields    格式：fieldname;datatypename;isRequired。如isRequired==false, 最后部分可以省略，如果datatypename＝＝string，且isRequired==false，可以只写fieldname
     * @return
     */
    public static MetaData createMetaData(String namespace, String name, String... fields) {
        MetaData metaData = new MetaData();
        metaData.setNameSpace(namespace);
        metaData.setConfigureName(name);
        if (fields == null || fields.length == 0) {
            metaData.toObject(metaData.toJson());
            return metaData;
        }
        List<MetaDataField> metaDataFieldList = new ArrayList<>();
        for (String field : fields) {
            MetaDataField metaDataField = new MetaDataField();
            String[] values = field.split(";");
            String fieldName = values[0];
            metaDataField.setFieldName(fieldName);
            DataType dataType = DataTypeUtil.getDataTypeFromClass(String.class);
            boolean isRequired = false;
            if (values.length > 1) {
                String dataTypeName = values[1];
                dataType = DataTypeUtil.getDataType(dataTypeName);

            }
            metaDataField.setDataType(dataType);
            if (values.length > 2) {
                isRequired = Boolean.valueOf(values[2]);

            }
            metaDataField.setIsRequired(isRequired);
            metaDataFieldList.add(metaDataField);
        }
        metaData.setMetaDataFields(metaDataFieldList);
        metaData.toObject(metaData.toJson());
        return metaData;
    }

}
