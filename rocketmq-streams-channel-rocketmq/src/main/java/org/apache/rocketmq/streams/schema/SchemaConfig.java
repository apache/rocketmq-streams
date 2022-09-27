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

package org.apache.rocketmq.streams.schema;

import java.io.Serializable;
import org.apache.commons.lang3.StringUtils;

public class SchemaConfig implements Serializable {

    /**
     * json, avro, protobuf, kyro, thrift, etc.
     */
    private String schemaType;

    /**
     * allowed to be null, if null means skip schema registry
     */
    private String schemaRegistryUrl;

    /**
     * class name of deserialize target object
     */
    private String targetClass;

    /**
     * class name of deserializer
     */
    private String deserializerClass;

    public SchemaConfig() {
    }

    public SchemaConfig(SchemaType schemaType) {
        this.schemaType = schemaType.name();
    }

    public SchemaConfig(SchemaType schemaType, Class targetClass) {
        this.schemaType = schemaType.name();
        this.targetClass = targetClass.getName();
    }

    public SchemaConfig(Class deserializerClass) {
        this.deserializerClass = deserializerClass.getName();
    }

    public SchemaConfig(SchemaType schemaType, Class targetClass, String schemaRegistryUrl) {
        this.schemaType = schemaType.name();
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.targetClass = targetClass.getName();
    }

    public String getSchemaType() {
        return schemaType;
    }

    public void setSchemaType(String schemaType) {
        this.schemaType = schemaType;
    }

    public String getSchemaRegistryUrl() {
        return schemaRegistryUrl;
    }

    public void setSchemaRegistryUrl(String schemaRegistryUrl) {
        this.schemaRegistryUrl = schemaRegistryUrl;
    }

    public String getTargetClass() {
        return targetClass;
    }

    public void setTargetClass(String targetClass) {
        this.targetClass = targetClass;
    }

    public String getDeserializerClass() {
        return deserializerClass;
    }

    public void setDeserializerClass(String deserializerClass) {
        this.deserializerClass = deserializerClass;
    }

    @Override
    public String toString() {
        return "SchemaConfig{" +
            "schemaType='" + schemaType + '\'' +
            ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
            ", targetClass='" + targetClass + '\'' +
            ", deserializerClass='" + deserializerClass + '\'' +
            '}';
    }

    public boolean equals(SchemaConfig configToCompare) {
        if (!StringUtils.equals(getSchemaType(), configToCompare.getSchemaType())) {
            return false;
        }
        if (!StringUtils.equals(getTargetClass(), configToCompare.getTargetClass())) {
            return false;
        }
        if (!StringUtils.equals(getSchemaRegistryUrl(), configToCompare.getSchemaRegistryUrl())) {
            return false;
        }
        if (!StringUtils.equals(getDeserializerClass(), configToCompare.getDeserializerClass())) {
            return false;
        }
        return true;
    }
}
