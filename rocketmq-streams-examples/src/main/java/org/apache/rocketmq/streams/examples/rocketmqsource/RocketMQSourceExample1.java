package org.apache.rocketmq.streams.examples.rocketmqsource;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;

public class RocketMQSourceExample1 {
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");

        source.fromRocketmq(
                RocketMQSourceExample2.RMQ_TOPIC,
                RocketMQSourceExample2.RMQ_CONSUMER_GROUP_NAME,
                RocketMQSourceExample2.NAMESRV_ADDRESS
        )
                .map(message -> message)
                .toPrint(1)
                .start();

    }
}
