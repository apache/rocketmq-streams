package org.apache.rocketmq.streams.examples.rocketmqsource;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;

import static org.apache.rocketmq.streams.examples.rocketmqsource.Constant.*;

public class RocketMQSourceExample4 {

    public static void main(String[] args) {
        System.out.println("send data to rocketmq");
        ProducerFromFile.produce("joinData-1.txt", NAMESRV_ADDRESS, RMQ_TOPIC);

        ProducerFromFile.produce("joinData-2.txt", NAMESRV_ADDRESS, RMQ_TOPIC + 2);

        try {
            Thread.sleep(1000 * 3);
        } catch (InterruptedException e) {
        }

        System.out.println("begin streams code");

        DataStream leftStream = StreamBuilder.dataStream("namespace", "name").fromRocketmq(
                RMQ_TOPIC,
                RMQ_CONSUMER_GROUP_NAME,
                true,
                NAMESRV_ADDRESS)
                .filter((JSONObject value) -> {
                    if (value.getString("ProjectName") != null && value.getString("LogStore") != null) {
                        return true;
                    }
                    return false;
                });


        DataStream rightStream = StreamBuilder.dataStream("namespace", "name2").fromRocketmq(
                RMQ_TOPIC + 2,
                RMQ_CONSUMER_GROUP_NAME + 2,
                true,
                NAMESRV_ADDRESS)
                .filter((JSONObject value) -> {
                    if (value.getString("ProjectName") != null && value.getString("LogStore") != null) {
                        return true;
                    }
                    return false;
                });

        leftStream.leftJoin(rightStream)
                .setCondition("(ProjectName,==,ProjectName)&(LogStore,==,LogStore)")
                .toDataSteam()
                .toPrint(1)
                .start();

        System.out.println("consumer end");
    }

}
