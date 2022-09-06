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
