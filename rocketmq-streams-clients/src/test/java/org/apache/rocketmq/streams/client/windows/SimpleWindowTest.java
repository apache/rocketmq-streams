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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.client.StreamExecutionEnvironment;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.functions.ForEachMessageFunction;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.junit.Test;

public class SimpleWindowTest {
    @Test
    public void testCountWindow() {
        int msgCount = 88121;
        StreamExecutionEnvironment.getExecutionEnvironment().create("namespace", "name")
            .fromFile("window_msg_" + msgCount + ".txt", true)
            .window(TumblingWindow.of(Time.seconds(5)))
            .groupBy("ProjectName")
            .count("c")
            .toDataSteam()
            .forEachMessage(new ForEachMessageFunction() {
                AtomicInteger count = new AtomicInteger(0);

                @Override public void foreach(IMessage message, AbstractContext context) {
                    int c = count.incrementAndGet();
                    message.getMessageBody().put("total", c);
                    message.getMessageBody().put("current_time", DateUtil.getCurrentTimeString());
                    if (c > (msgCount - 10)) {
                        System.out.println(message.getMessageBody());
                    }

                }
            })
            .start();
    }
}
