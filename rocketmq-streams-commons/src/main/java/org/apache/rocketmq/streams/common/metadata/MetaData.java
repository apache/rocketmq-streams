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
package org.apache.rocketmq.streams.common.metadata;

import com.alibaba.fastjson.JSONObject;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.datatype.DataType;
import org.apache.rocketmq.streams.common.datatype.MapDataType;
import org.apache.rocketmq.streams.common.utils.DataTypeUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class MetaData extends AbstractMetaData {

    private static final String PRI_KEY = "id";

    private Long id;

    private String idFieldName;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getIdFieldName() {
        return idFieldName;
    }

    public void setIdFieldName(String idFieldName) {
        this.idFieldName = idFieldName;
    }

    @Override
    protected void setJsonValue(JSONObject jsonObject) {
        this.idFieldName = jsonObject.getString("idFieldName");
    }

    @Override
    protected void getJsonValue(JSONObject jsonObject) {
        jsonObject.put("idFieldName", this.idFieldName);
    }

    public static MetaData createMetaData(ResultSet metaResult) throws SQLException {
        MetaData metaData = new MetaData();
        if (metaResult == null) {
            return metaData;
        }
        while (metaResult.next()) {
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setFieldName(metaResult.getString("COLUMN_NAME"));
            metaDataField.setDataType(DataTypeUtil.createDataTypeByDbType(metaResult.getString("TYPE_NAME")));
            metaData.getMetaDataFields().add(metaDataField);
        }
        return metaData;
    }

    /**
     * 根据json，创建metadata
     *
     * @param msg
     * @return
     */
    public static MetaData createMetaData(JSONObject msg) {
        MetaData metaData = new MetaData();
        if (msg == null) {
            return metaData;
        }
        Map<String, Object> data = msg;

        Iterator<Map.Entry<String, Object>> it = data.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Object> entry = it.next();
            String fieldName = entry.getKey();
            Object value = entry.getValue();
            MetaDataField metaDataField = new MetaDataField();
            metaDataField.setFieldName(fieldName);
            if (value != null) {
                DataType dataType = DataTypeUtil.getDataTypeFromClass(value.getClass());
                if (dataType != null) {
                    metaDataField.setDataType(dataType);
                }
            } else {
                metaDataField.setIsRequired(false);
            }
            metaData.getMetaDataFields().add(metaDataField);
        }
        return metaData;
    }

    public static void main(String[] args) {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", new String[] {"chris", "111"});
        jsonObject.put("age", 18);
        MetaData metaData = createMetaData(jsonObject);
        System.out.println(metaData.toJson());
    }

    /**
     * 创建meta信息
     *
     * @param object
     * @param paras
     * @return
     */
    public static MetaData createMetaDate(Object object, Map<String, Object> paras) {
        MetaData metaData = new MetaData();
        String tableName = getTableName(object.getClass());
        metaData.setTableName(tableName);
        Set<String> fieldNames = new HashSet<>();
        ReflectUtil.scanFields(object, (o, field) -> {
            String fieldName = field.getName();
            if (fieldNames.contains(fieldName)) {
                return;
            }
            fieldNames.add(fieldName);
            /**id默认主键**/
            if (PRI_KEY.equals(fieldName)) {
                metaData.setIdFieldName(fieldName);
            }
            MetaDataField metaDataField = new MetaDataField();
            String columnName = getColumnNameFromFieldName(fieldName);
            metaDataField.setFieldName(columnName);
            Object value = ReflectUtil.getBeanFieldValue(o, fieldName);
            DataType datatype = DataTypeUtil.createFieldDataType(o, fieldName);
            metaDataField.setDataType(datatype);
            //TODO
            if (datatype instanceof MapDataType) {
                paras.put(columnName, value.toString());
            } else {
                paras.put(columnName, value);
            }
            metaData.getMetaDataFields().add(metaDataField);
        });
        return metaData;
    }

    public static String getTableName(Class clazz) {
        TableClassName tableClass = (TableClassName) clazz.getAnnotation(TableClassName.class);
        if (tableClass != null) {
            String className = tableClass.value();
            if (StringUtil.isNotEmpty(className)) {
                return getColumnNameFromFieldName(className);
            }
        }
        return getColumnNameFromFieldName(clazz.getSimpleName());
    }

    /**
     * 把驼峰转换成下划线的形式
     *
     * @param para
     * @return
     */
    public static String getColumnNameFromFieldName(String para) {
        StringBuilder sb = new StringBuilder(para);
        boolean isStartHump = false;
        //定位
        int temp = 0;
        if (Character.isUpperCase(para.charAt(0))) {
            isStartHump = true;
            char word = para.charAt(0);
            word += 32;
            sb.setCharAt(0, word);
        }
        for (int i = 1; i < para.length(); i++) {
            if (Character.isUpperCase(para.charAt(i))) {
                if (isStartHump == false) {
                    char word = para.charAt(i);
                    word += 32;
                    sb.setCharAt(i + temp, word);
                    sb.insert(i + temp, "_");
                    temp += 1;
                    isStartHump = true;
                }
            } else {
                isStartHump = false;
            }
        }
        return sb.toString();
    }

    /**
     * 如果某个类，不想自己的类名转化成表名，可以指定类名
     */
    @Target(ElementType.TYPE)
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface TableClassName {
        /**
         * 期望做表名的class的simplename
         *
         * @return
         */
        String value() default "";

    }

}
