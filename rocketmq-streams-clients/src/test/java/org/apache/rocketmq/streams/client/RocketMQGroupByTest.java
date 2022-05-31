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
package org.apache.rocketmq.streams.client;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.functions.ForEachMessageFunction;
import org.junit.Test;

public class RocketMQGroupByTest implements Serializable {

    @Test
    public void testRocketMq() throws Exception {
        DataStreamSource.create("tmp", "tmp")
            .fromRocketmq("dipper_test_write_merge5", "dipper_group", true, "localhost:9876", null)
            .window(TumblingWindow.of(Time.seconds(5)))
            .groupBy("host_uuid", "cmdline")
            .count("c")
            .toDataSteam()
            .forEachMessage(new ForEachMessageFunction() {
                protected AtomicLong COUNT = new AtomicLong(0);
                protected Long start = null;

                @Override public void foreach(IMessage message, AbstractContext context) {
                    if (start == null) {
                        start = System.currentTimeMillis();
                    }
                    double count = COUNT.incrementAndGet();
                    double timeGap = (System.currentTimeMillis() - start) / 1000;
                    if (timeGap == 0) {
                        timeGap = 1;
                    }
                    Double qps = count / timeGap;
                    System.out.println("qps is " + qps.longValue());
                }
            })
            .start();
    }
}
