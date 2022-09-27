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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.schema.registry.client.serde.Deserializer;

public class CustomSchemaWrapper implements SchemaWrapper {

    private static final Log LOG = LogFactory.getLog(CustomSchemaWrapper.class);

    private Deserializer deserializer;

    private SchemaConfig schemaConfig;

    public CustomSchemaWrapper(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        try {
            this.deserializer =
                (Deserializer)Class.forName(schemaConfig.getDeserializerClass()).newInstance();
        } catch (Exception e) {
            LOG.error("init CustomSchemaWrapper failed, " + schemaConfig.toString(), e);
            throw new RuntimeException("init CustomSchemaWrapper failed");
        }
    }

    @Override
    public Object deserialize(MessageExt messageExt) {
        return deserializer.deserialize(messageExt.getTopic(), messageExt.getBody());
    }

    @Override
    public SchemaConfig getConfig() {
        return schemaConfig;
    }
}
