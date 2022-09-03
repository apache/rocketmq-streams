package org.apache.rocketmq.streams.schema;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClient;
import org.apache.rocketmq.schema.registry.client.SchemaRegistryClientFactory;
import org.apache.rocketmq.schema.registry.client.config.AvroSerializerConfig;
import org.apache.rocketmq.schema.registry.client.serde.avro.SpecificAvroSerde;

public class AvroSchemaWrapper implements SchemaWrapper {

    private static final Log LOG = LogFactory.getLog(AvroSchemaWrapper.class);

    private SpecificAvroSerde avroSerde;

    private SchemaConfig schemaConfig;

    public AvroSchemaWrapper(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
        try {
            if (schemaConfig.getSchemaRegistryUrl() == null) {
                avroSerde = new SpecificAvroSerde();
                Map<String, Object> configs = new HashMap<>();
                configs.put(AvroSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getClassName()));
                configs.put(AvroSerializerConfig.SKIP_SCHEMA_REGISTRY, true);
                avroSerde.configure(configs);
            } else {
                SchemaRegistryClient schemaRegistryClient =
                    SchemaRegistryClientFactory.newClient(schemaConfig.getSchemaRegistryUrl(), null);
                avroSerde = new SpecificAvroSerde(schemaRegistryClient);
                Map<String, Object> configs = new HashMap<>();
                configs.put(AvroSerializerConfig.DESERIALIZE_TARGET_TYPE, Class.forName(schemaConfig.getClassName()));
                avroSerde.configure(configs);
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
        return avroSerde.deserializer().deserialize(subject, msgBody);
    }

    @Override
    public SchemaConfig getConfig() {
        return schemaConfig;
    }
}
