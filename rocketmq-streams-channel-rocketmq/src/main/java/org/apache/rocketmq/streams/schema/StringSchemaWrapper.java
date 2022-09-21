package org.apache.rocketmq.streams.schema;

import org.apache.rocketmq.common.message.MessageExt;

public class StringSchemaWrapper implements SchemaWrapper {

    private SchemaConfig schemaConfig;

    public StringSchemaWrapper(SchemaConfig schemaConfig) {
        this.schemaConfig = schemaConfig;
    }

    @Override
    public Object deserialize(MessageExt messageExt) {
        byte[] msgBody = messageExt.getBody();
        return new String(msgBody);
    }

    @Override
    public SchemaConfig getConfig() {
        return schemaConfig;
    }
}
