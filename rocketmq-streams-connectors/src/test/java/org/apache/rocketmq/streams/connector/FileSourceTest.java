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
package org.apache.rocketmq.streams.connector;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.connectors.source.impl.FilesPullSource;
import org.junit.Test;

public class FileSourceTest {
    @Test
    public void testPullFileSource() throws InterruptedException {
        FilesPullSource filesPullSource = new FilesPullSource();
        filesPullSource.setFilePath("/Users/junjie.cheng/Desktop/TEST_DATA");
        //filesPullSource.setTest(true);
        //filesPullSource.setCloseBalance(true);
        filesPullSource.setName("test");
        filesPullSource.setMaxThread(3);
        //filesPullSource.setPullSize(2);
        filesPullSource.setPullIntervalMs(100);
        filesPullSource.setCheckpointTime(1000 * 10);
        filesPullSource.init();
        ConcurrentHashMap<String, AtomicInteger> split2Count = new ConcurrentHashMap<>();
        filesPullSource.start(new IStreamOperator() {
            @Override public Object doMessage(IMessage message, AbstractContext context) {
                if (message.getHeader().isSystemMessage()) {
                    return null;
                }
//                try {
//                    Thread.sleep(10000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
                synchronized (this) {
                    AtomicInteger count = split2Count.get(message.getHeader().getQueueId());
                    if (count == null) {
                        count = new AtomicInteger(1);
                        split2Count.putIfAbsent(message.getHeader().getQueueId(), count);
                        System.out.println(DateUtil.getCurrentTime() + " " + message.getHeader().getQueueId() + ":" + count.get());
                        return null;
                    }

                    System.out.println(DateUtil.getCurrentTime() + " " + message.getHeader().getQueueId() + ":" + count.incrementAndGet());
                    return null;
                }
//                System.out.println(DateUtil.getCurrentTime()+" "+message.getMessageBody());
//                return null;

            }
        });

        while (true) {
            Thread.sleep(1000);
        }

    }

    @Test
    public void testWriteRows() {
        List<String> rows = new ArrayList<>();
        for (int i = 1; i < 101; i++) {
            JSONObject row = new JSONObject();
            row.put("num", i);
            rows.add(row.toJSONString());
        }
        FileUtil.write("/tmp/chris/test.txt", rows);
    }
}
