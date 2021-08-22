package org.apache.rocketmq.streams.examples.rocketmqsource;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.source.RocketMQSource;

public class RocketMQSourceExample2 {
    public static final String NAMESRV_ADDRESS = "47.102.136.138:9876";
    public static final String RMQ_TOPIC = "topic_tiger_0822_01";
    public static final String RMQ_CONSUMER_GROUP_NAME = "consumer_tiger_0822_01";
    public static final String TAGS = "*";

    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");

        source.from(new RocketMQSource(
                RMQ_TOPIC,
                TAGS,
                RMQ_CONSUMER_GROUP_NAME,
                "",
                NAMESRV_ADDRESS,
                "",
                "",
                ""
        ))
                .map(message -> message)
                .toPrint(1)
                .start();

    }
}
