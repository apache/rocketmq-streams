package org.apache.rocketmq.streams.schema;

public enum SchemaType {

    /**
     * Avro type
     */
    AVRO("AVRO"),
    /**
     * Protobuf type
     */
    PROTOBUF("PROTOBUF"),
    /**
     * Thrift type
     */
    THRIFT("THRIFT"),
    /**
     * Json type
     */
    JSON("JSON"),
    /**
     * Text type for reserved
     */
    TEXT("TEXT"),
    /**
     * Binlog type for reserved
     */
    BINLOG("BINLOG");

    private final String value;

    SchemaType(final String value) {
        this.value = value;
    }
}
