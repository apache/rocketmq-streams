package org.apache.rocketmq.streams.client;

import java.io.BufferedReader;
import java.io.FileReader;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

public class OnewayProducer {
    public static void main(String[] args) throws Exception {
        BufferedReader bufferedReader = new BufferedReader(new FileReader("file://file_path/xxxxxxxx.json"));
        String line = bufferedReader.readLine();

        DefaultMQProducer producer = new DefaultMQProducer("producer_xxxx01");
        producer.setNamesrvAddr("localhost:9876");
        producer.start();

        while (line != null) {
            Message msg = new Message("topic_xxxx01",
                "TagA",
                line.getBytes(RemotingHelper.DEFAULT_CHARSET)
            );
            producer.sendOneway(msg);
            line = bufferedReader.readLine();
        }

        Thread.sleep(5000);
        producer.shutdown();
    }
}
