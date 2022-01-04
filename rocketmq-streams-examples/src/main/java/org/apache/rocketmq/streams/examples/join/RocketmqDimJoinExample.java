package org.apache.rocketmq.streams.examples.join;

import org.apache.rocketmq.streams.client.StreamBuilder;

public class RocketmqDimJoinExample {

    public static void main(String[] args) {
        StreamBuilder.dataStream("tmp", "tmp")
            .fromRocketmq("TopicTest", "groupA", true, "localhost:9876")
            .dimJoin("classpath://dim.txt", 10000L)
            .on("ProjectName, =, project")
            .toDataSteam()
            .toPrint()
            .start();
    }

}
