/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.streams.client.windows;

import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.junit.Test;

public class WindowFromRocketMQ extends AbstractWindowFireModeTest {

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
    public void testWindowToRocketMQ() throws InterruptedException {

        long start=System.currentTimeMillis();
        StreamBuilder.dataStream("namespace", "name")
            .fromFile("/Users/yuanxiaodong/chris/sls_10.txt", true)
                .toRocketmq(topic, "chris1", "", "", "", "", "")
            .start();
    }


    protected DataStream createSourceDataStream(){
        return  StreamBuilder.dataStream("namespace", "name1")
            .fromRocketmq(topic,"chris1","");
    }


}
