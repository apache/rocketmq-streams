package org.apache.rocketmq.streams.schema;

import org.apache.rocketmq.common.message.MessageExt;

public interface SchemaWrapper {

    Object deserialize(MessageExt messageExt);

    SchemaConfig getConfig();
}
