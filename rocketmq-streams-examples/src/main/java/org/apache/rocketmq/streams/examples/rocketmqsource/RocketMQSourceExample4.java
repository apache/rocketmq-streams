package org.apache.rocketmq.streams.examples.rocketmqsource;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.JoinStream;
import org.apache.rocketmq.streams.client.transform.window.Time;

import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.*;

public class RocketMQSourceExample4 {

    public static void main(String[] args) {
        System.out.println("send data to rocketmq");
        ProducerFromFile.produce("data.txt", NAMESRV_ADDRESS, RMQ_TOPIC);

        ProducerFromFile.produce("data.txt", NAMESRV_ADDRESS, RMQ_TOPIC + 2);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }

        System.out.println("begin streams code");

        DataStream leftStream = StreamBuilder.dataStream("namespace", "name").fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                true,
                NAMESRV_ADDRESS);


        DataStream rightStream = StreamBuilder.dataStream("namespace", "name2").fromRocketmq(
                RMQ_TOPIC + 2,
                RMQ_CONSUMER_GROUP_NAME + 2,
                true,
                NAMESRV_ADDRESS);

        leftStream.join(rightStream)
                .setJoinType(JoinStream.JoinType.LEFT_JOIN)
                .setCondition("InFlow,==,InFlow")
                .window(Time.minutes(1))
                .toDataSteam()
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();

        System.out.println("consumer end");
    }

}
