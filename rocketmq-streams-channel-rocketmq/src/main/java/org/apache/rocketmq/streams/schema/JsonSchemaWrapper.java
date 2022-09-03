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
                configs.put(JsonSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getClassName()));
                configs.put(JsonSerializerConfig.SKIP_SCHEMA_REGISTRY, true);
                jsonSerde.configure(configs);
            } else {
                SchemaRegistryClient schemaRegistryClient =
                    SchemaRegistryClientFactory.newClient(schemaConfig.getSchemaRegistryUrl(), null);
                jsonSerde = new JsonSerde(schemaRegistryClient);
                Map<String, Object> configs = new HashMap<>();
                configs.put(JsonSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getClassName()));
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
