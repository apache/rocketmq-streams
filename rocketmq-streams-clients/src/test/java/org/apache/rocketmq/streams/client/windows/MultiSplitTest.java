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

import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.client.transform.DataStream;
import org.apache.rocketmq.streams.common.functions.MapFunction;
import org.apache.rocketmq.streams.common.topology.IWindow;
import org.junit.Test;

public class MultiSplitTest extends SingleSplitTest {
    protected String topic = "TOPIC_DIPPER_SYSTEM_MSG_6";
    protected String group = "PID_DIPPER";
    protected String nameServerAddress = "127.0.0.1:9876";

    protected DataStream createSourceDataStream() {

        return StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name1")
            .fromRocketmq(topic, "window_test", true, null);
    }

    protected int getSourceCount() {
        return 10;
    }

    /**
     * validate the window result  meet expectations
     */
    @Test
    public void testWindowResult() {
        super.testWindowResult(getSourceCount());
    }

    /**
     * @throws InterruptedException
     */
    @Test
    public void testFireMode0() throws InterruptedException {

        super.executeWindowStream(true, 5, IWindow.DEFAULTFIRE_MODE, 0, 20l);
    }

    @Test
    public void testFireMode1() throws InterruptedException {
        super.executeWindowStream(false, 5, IWindow.MULTI_WINDOW_INSTANCE_MODE, 0, 20l);
    }

    @Test
    public void testMutilWindow() {
        super.testMutilWindow(false);
    }

    @Test
    public void testRoketmqConsumner() {
        AtomicInteger count = new AtomicInteger(0);
        createSourceDataStream().map(new MapFunction<JSONObject, JSONObject>() {

            @Override
            public JSONObject map(JSONObject message) throws Exception {
                System.out.println(count.incrementAndGet());
                return message;
            }
        }).start();
    }

    @Test
    public void testInsertWindowMsg() {
        StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name1")
            .fromFile(filePath, true).toRocketmq(topic, group, nameServerAddress).start();
    }

}
