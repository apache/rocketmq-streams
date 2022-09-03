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
     * deserialize target class
     */
    private String className;

    public SchemaConfig() {
    }

    public SchemaConfig(SchemaType schemaType, Class targetClass) {
        this.schemaType = schemaType.name();
        this.className = targetClass.getName();
    }

    public SchemaConfig(SchemaType schemaType, Class targetClass, String schemaRegistryUrl) {
        this.schemaType = schemaType.name();
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.className = targetClass.getName();
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

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    @Override
    public String toString() {
        return "SchemaConfig{" +
            "schemaType='" + schemaType + '\'' +
            ", schemaRegistryUrl='" + schemaRegistryUrl + '\'' +
            ", className='" + className + '\'' +
            '}';
    }

    public boolean equals(SchemaConfig configToCompare) {
        if (!StringUtils.equals(getSchemaType(), configToCompare.getSchemaType())) {
            return false;
        }
        if (!StringUtils.equals(getClassName(), configToCompare.getClassName())) {
            return false;
        }
        if (!StringUtils.equals(getSchemaRegistryUrl(), configToCompare.getSchemaRegistryUrl())) {
            return false;
        }
        return true;
    }
}
