package org.apache.rocketmq.streams.client;

import com.alibaba.fastjson.JSONObject;

import com.google.gson.JsonObject;
import org.apache.rocketmq.streams.common.channel.impl.file.FileSink;
import org.apache.rocketmq.streams.common.context.Message;
import org.junit.Test;

public class SinkTest {
    @Test
    public void testSink(){
        FileSink fileSink=new FileSink("/tmp/file.txt");
        fileSink.setBatchSize(1000);
        fileSink.init();
        fileSink.openAutoFlush();
        for(int i=0;i<100;i++){
            JSONObject msg=new JSONObject();
            msg.put("name","chris"+i);
            fileSink.batchAdd(new Message(msg));
        }
        fileSink.flush();
    }
}
