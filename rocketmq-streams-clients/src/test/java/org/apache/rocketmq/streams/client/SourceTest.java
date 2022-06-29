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
import org.apache.rocketmq.streams.common.channel.impl.file.FileSource;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.functions.ForEachMessageFunction;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.junit.Test;

public class SourceTest implements Serializable {

    @Test
    public void testSource() {
        FileSource source = new FileSource("/tmp/aegis_proc_public.txt");
        source.setJsonData(true);
        source.init();
        source.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage message, AbstractContext context) {
                System.out.println(message.getMessageBody());
                return null;
            }
        });
    }

    @Test
    public void testoutputdata() {
        AtomicLong count = new AtomicLong(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        DataStreamSource.create("tmp", "tmp")
            .fromFile("/tmp/aegis_proc_public.txt")
            .toRocketmq("dipper_test_1", "dipper_group", "localhost:9876")
            .forEachMessage(new ForEachMessageFunction() {
                @Override public void foreach(IMessage message, AbstractContext context) {
                    long c = count.incrementAndGet();
                    long cost = (System.currentTimeMillis() - startTime.get()) / 1000;
                    if (cost == 0) {
                        cost = 1;
                    }
                    double qps = ((double) c) / ((double) cost);
                    if (c % 1000 == 0) {
                        System.out.println("qps is " + qps);
                    }

                }
            })
            .start();
    }

    @Test
    public void testImportMsgFromFile() {
        AtomicLong count = new AtomicLong(0);
        AtomicLong startTime = new AtomicLong(System.currentTimeMillis());
        DataStreamSource.create("tmp", "tmp")
            .fromRocketmq("dipper_test_1", "dipper_group1", true, "localhost:9876")
            .forEachMessage(new ForEachMessageFunction() {
                @Override public void foreach(IMessage message, AbstractContext context) {
                    System.out.println(message.getMessageBody());
                    long c = count.incrementAndGet();
                    long cost = (System.currentTimeMillis() - startTime.get()) / 1000;
                    if (cost == 0) {
                        cost = 1;
                    }
                    double qps = ((double) c) / ((double) cost);
                    if (c % 1000 == 0) {
                        System.out.println("qps is " + qps);
                    }

                }
            })
            .start();
    }


}
