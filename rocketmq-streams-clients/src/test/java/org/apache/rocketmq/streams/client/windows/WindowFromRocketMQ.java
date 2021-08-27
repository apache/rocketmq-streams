package org.apache.rocketmq.streams.client.windows;

import com.alibaba.fastjson.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.functions.ForEachMessageFunction;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.sink.RocketMQSink;
import org.junit.Test;

public class WindowFromRocketMQ extends AbstractWindowFireModeTest {

    String topic = "TOPIC_DIPPER_SYSTEM_MSG_6";
    @Test
    public void testWindowFireMode0() throws InterruptedException {
        FileUtil.deleteFile("/tmp/rockstmq-streams");

        //ComponentCreator.getProperties().setProperty(" window.debug.rocksdb","true");
//        ComponentCreator.getProperties().setProperty("metq.debug.switch","true");
//        ComponentCreator.getProperties().setProperty("window.debug","true");
//        ComponentCreator.getProperties().setProperty("window.debug.dir","/tmp/rockstmq-streams");
//        ComponentCreator.getProperties().setProperty("window.debug.countFileName","total");
        super.testWindowFireMode0(false);
    }



    @Test
    public void testWindowFireMode1() throws InterruptedException {
        ComponentCreator.setProperties("dipper.properties");
        super.testWindowFireMode1(false);
    }



    @Test
    public synchronized void testWindowFireMode2() {
        super.testWindowFireMode2(true);
    }


    @Test
    public void testWindowToMetaq() throws InterruptedException {
        long start=System.currentTimeMillis();
        StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/yuanxiaodong/chris/sls_100000.txt", true)
            .toRocketmq(topic)
            .start();
    }


    protected DataStream createSourceDataStream(){
        return  StreamBuilder.dataStream("namespace", "name1")
            .fromRocketmq(topic,"chris1",true);
    }


    @Test
    public void testMetaqSink() throws InterruptedException {
         RocketMQSink sink = new RocketMQSink();
        sink.setTopic(topic);
        sink.setSplitNum(5);
        sink.init();
        System.out.println(sink.getSplitNum());
        JSONObject msg=new JSONObject();
        msg.put("name","age");
        sink.batchAdd(new Message(msg));
        sink.flush();
        Thread.sleep(1500000000);
    }


    @Test
    public void testMetaqConsumer(){
        Map<String,String> queueId2EventTime=new HashMap<>();
        AtomicInteger count=new AtomicInteger(0);
        StreamBuilder.dataStream("namespace", "name1")
            .fromRocketmq(topic,"chris1",true)
            .forEachMessage(new ForEachMessageFunction() {
                @Override public void foreach(IMessage message, AbstractContext context) {
                    System.out.println(count.incrementAndGet());

//                    String existEventTime=queueId2EventTime.get(queueId);
//
//                    if(existEventTime==null){
//                        existEventTime=eventTime;
//                    }else {
//                        if(existEventTime.compareTo(eventTime)>0){
//
//                            Assert.assertTrue(false);
//                        }
//                        existEventTime=eventTime;
//                    }
//                    queueId2EventTime.put(queueId,existEventTime);
                }
            }).start();
    }

    @Test
    public void testRocketmq(){

    }

}
