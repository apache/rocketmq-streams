package org.apache.rocketmq.streams.common.cache;

import java.io.Serializable;
import java.util.Arrays;

/**
 * @program rocketmq-streams-apache
 * @description
 */
public class TableSchema implements Serializable {

    String[] fieldName;
    String[] fieldType;

    public TableSchema(String[] fields, String[] fieldType) {
        assert fields.length == fieldType.length : "length not match, " + Arrays.toString(fields) + ", " + Arrays.toString(fieldType);
        this.fieldName = fields;
        this.fieldType = fieldType;
    }

    public String[] getFieldName() {
        return fieldName;
    }

    public void setFieldName(String[] fieldName) {
        this.fieldName = fieldName;
    }

    public String[] getFieldType() {
        return fieldType;
    }

    public void setFieldType(String[] fieldType) {
        this.fieldType = fieldType;
    }

    public int schemaLength() {
        return this.fieldName.length;
    }

    public String getField(int index) {
        return fieldName[index];
    }

    public String getFieldType(int index) {
        return fieldType[index];
    }
}
