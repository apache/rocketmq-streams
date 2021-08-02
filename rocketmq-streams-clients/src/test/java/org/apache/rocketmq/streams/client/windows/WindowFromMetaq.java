package org.apache.rocketmq.streams.client.windows;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.junit.Test;

public class WindowFromMetaq extends AbstractWindowFireModeTest {

    String topic = "TOPIC_DIPPER_SYSTEM_MSG_5";
    @Test
    public void testWindowFireMode0() throws InterruptedException {
        super.testWindowFireMode0(true);
    }



    @Test
    public void testWindowFireMode1() throws InterruptedException {
        super.testWindowFireMode1(true);
    }



    @Test
    public void testWindowFireMode2() {
        super.testWindowFireMode2(true);
    }


    @Test
    public void testWindowToMetaq() throws InterruptedException {

        long start=System.currentTimeMillis();
        StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/yuanxiaodong/chris/sls_10.txt", true)
            .toMetaq(topic)
            .start();
    }


    protected DataStream createSourceDataStream(){
        return  StreamBuilder.dataStream("namespace", "name1")
            .fromRocketmq(topic,"chris1",true);
    }


}
