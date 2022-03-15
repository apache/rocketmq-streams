package org.apache.rocketmq.streams.client.example;

import com.alibaba.fastjson.JSONObject;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.transform.SplitStream;
import org.apache.rocketmq.streams.common.functions.SplitFunction;

public class SplitTest {

    public static void main(String[] args) throws InterruptedException {
        DataStreamSource dataStream = DataStreamSource.create("namespace", "name");
        SplitStream ds = StreamBuilder.dataStream("tmp", "tmp")
            .fromFile("window_msg_10.txt", true)
            .split(new SplitFunction<JSONObject>() {
                @Override public String split(JSONObject o) {
                    return o.getString("ProjectName");
                }
            });

        ds.select("project-2").toPrint();
        ds.select("project-7").toPrint();
        ds.select("project-1").toPrint();

        ds.toDataStream().start();

    }

}
