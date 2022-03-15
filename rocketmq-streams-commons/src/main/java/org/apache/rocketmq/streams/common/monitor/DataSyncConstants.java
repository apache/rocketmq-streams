package org.apache.rocketmq.streams.common.monitor;

public class DataSyncConstants {

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public static final String RULE_UP_TOPIC = "dipper.console.topic.up";

    /**
     * rocketmq-stream更新模块对应的topic
     */
    public static final String RULE_DOWN_TOPIC = "dipper.console.topic.down";

    /**
     * rocketmq-stream更新模块对应的tag
     */
    public static final String RULE_TOPIC_TAG = "dipper.console.tag";

    /**
     * rocketmq-stream更新模块对应的消息渠道类型 默认为metaq
     */
    public static final String UPDATE_TYPE = "dipper.console.service.type";

    public static final String UPDATE_TYPE_HTTP = "http";
    public static final String UPDATE_TYPE_DB = "db";
    public static final String UPDATE_TYPE_ROCKETMQ = "rocketmq";
}
