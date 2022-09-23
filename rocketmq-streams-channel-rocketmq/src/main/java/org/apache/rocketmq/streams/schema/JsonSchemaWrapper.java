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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.config.JsonSerializerConfig;
import org.apache.rocketmq.schema.registry.client.serde.json.JsonSerde;

/**
 * @author huitong
 */
public class JsonSchemaWrapper implements SchemaWrapper {

    private static final Log LOG = LogFactory.getLog(JsonSchemaWrapper.class);

    private JsonSerde jsonSerde;

    private SchemaConfig schemaConfig;

    public JsonSchemaWrapper(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        try {
            if (schemaConfig.getSchemaRegistryUrl() == null) {
                jsonSerde = new JsonSerde();
                Map<String, Object> configs = new HashMap<>();
                configs.put(JsonSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getTargetClass()));
                configs.put(JsonSerializerConfig.SKIP_SCHEMA_REGISTRY, true);
                jsonSerde.configure(configs);
            } else {
                SchemaRegistryClient schemaRegistryClient =
                    SchemaRegistryClientFactory.newClient(schemaConfig.getSchemaRegistryUrl(), null);
                jsonSerde = new JsonSerde(schemaRegistryClient);
                Map<String, Object> configs = new HashMap<>();
                configs.put(JsonSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getTargetClass()));
                jsonSerde.configure(configs);
            }
        } catch (Exception e) {
            LOG.error("init AvroSchemaWrapper failed, " + schemaConfig.toString(), e);
            throw new RuntimeException("init AvroSchemaWrapper failed");
        }
    }

    @Override
    public Object deserialize(MessageExt messageExt) {

        String subject = messageExt.getTopic();
        byte[] msgBody = messageExt.getBody();
        return jsonSerde.deserializer().deserialize(subject, msgBody);
    }

    @Override
    public SchemaConfig getConfig() {
        return schemaConfig;
    }
}
