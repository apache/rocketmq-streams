package org.apache.rocketmq.streams.examples.rocketmqsource;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.client.transform.JoinStream;
import org.apache.rocketmq.streams.common.functions.FilterFunction;

import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.NAMESRV_ADDRESS;
import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.RMQ_CONSUMER_GROUP_NAME;
import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.RMQ_TOPIC;

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

        DataStream leftStream = StreamBuilder.dataStream("namespace", "name").fromRocketmq(RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                false,
                NAMESRV_ADDRESS)
                .filter((FilterFunction<String>) value -> JSONObject.parseObject(value).getString("InFlow") != null);


        DataStream rightStream = StreamBuilder.dataStream("namespace", "name2").fromRocketmq(RMQ_TOPIC + 2,
                RMQ_CONSUMER_GROUP_NAME + 2,
                false,
                NAMESRV_ADDRESS)
                .filter((FilterFunction<String>) value -> JSONObject.parseObject(value).getString("InFlow") != null);

        leftStream.join(rightStream).setJoinType(JoinStream.JoinType.LEFT_JOIN)
                .setCondition("InFlow,==,InFlow")
                .toDataSteam()
                .map(message -> {
                    System.out.println(message);
                    return message + "--";
                })
                .toPrint(1)
                .start();

        System.out.println("consumer end");
    }

}
