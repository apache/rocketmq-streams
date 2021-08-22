package org.apache.rocketmq.streams.examples.filesource;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;

public class FileSourceExample {
    public static void main(String[] args) {
        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");
        source.fromFile("/your/file/path", false)
                .map(message -> message)
                .toPrint(1)
                .start();
    }
}
