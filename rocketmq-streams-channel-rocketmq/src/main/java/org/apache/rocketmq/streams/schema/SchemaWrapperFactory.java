package org.apache.rocketmq.streams.schema;

import java.util.HashMap;
import java.util.Map;

public class SchemaWrapperFactory {

    private static Map<String /*topic*/, SchemaWrapper> schemaWrapperCache = new HashMap<>();

    public static SchemaWrapper createIfAbsent(String topic, SchemaConfig schemaConfig) {
        SchemaWrapper schemaWrapper = schemaWrapperCache.get(topic);
        if (schemaWrapper != null) {
            // if change config, also need to create a new one
            if (schemaConfig.equals(schemaWrapper.getConfig())) {
                return schemaWrapper;
            }
        }
        if (SchemaType.JSON.name().equals(schemaConfig.getSchemaType())) {
            JsonSchemaWrapper jsonSchemaWrapper = new JsonSchemaWrapper(schemaConfig);
            schemaWrapperCache.put(topic, jsonSchemaWrapper);
            return jsonSchemaWrapper;
        } else if (SchemaType.AVRO.name().equals(schemaConfig.getSchemaType())) {
            AvroSchemaWrapper avroSchemaWrapper = new AvroSchemaWrapper(schemaConfig);
            schemaWrapperCache.put(topic, avroSchemaWrapper);
            return avroSchemaWrapper;
        } else {
            throw new RuntimeException("scheme type " + schemaConfig.getSchemaType() + " not supported");
        }
    }

}
