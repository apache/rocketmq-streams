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
