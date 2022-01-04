package org.apache.rocketmq.streams.examples.join;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;

public class RocketmqJoinExample {
    public static void main(String[] args) {
        DataStream left = StreamBuilder.dataStream("tmp", "tmp")
            .fromRocketmq("TopicTest", "groupA", true, "localhost:9876");
        DataStream right = StreamBuilder.dataStream("tmp", "tmp")
            .fromRocketmq("TopicTest", "groupB", true, "localhost:9876");

        left.join(right)
            .on("(ProjectName,=,ProjectName)")
            .toDataSteam()
            .toPrint()
            .start();
    }

}
