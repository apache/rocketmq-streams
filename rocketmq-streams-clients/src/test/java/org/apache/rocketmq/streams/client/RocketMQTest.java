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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MessageCache;
import org.apache.rocketmq.streams.common.channel.sinkcache.impl.MultiSplitMessageCache;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.functions.FlatMapFunction;
import org.apache.rocketmq.streams.common.functions.ForEachMessageFunction;
import org.apache.rocketmq.streams.sink.RocketMQSink;
import org.junit.Test;

public class RocketMQTest implements Serializable {

    @Test
    public void testCommonWrite() {
        DataStreamSource.create("tmp", "tmp")
            .fromFile("/tmp/aegis_proc_public.txt", true)

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
            }).toRocketmq("dipper_test_write_merge5", "localhost:9876")
            .start();
    }

    @Test
    public void testWrite() {
        DataStreamSource.create("tmp", "tmp")
            .fromFile("/tmp/aegis_proc_public.txt", true)

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
            .forEachMessage(new ForEachMessageFunction() {
                MultiSplitMessageCache messageCache = null;
                RocketMQSink sink = new RocketMQSink("localhost:9876", "dipper_test_write_merge4");

                @Override public void foreach(IMessage message, AbstractContext context) {
                    if (messageCache == null) {
                        synchronized (this) {
                            if (messageCache == null) {
                                sink.init();
                                messageCache = new MultiSplitMessageCache(new IMessageFlushCallBack<IMessage>() {

                                    @Override public boolean flushMessage(List<IMessage> messages) {

                                        JSONObject newMessage = new JSONObject();
                                        JSONArray jsonArray = new JSONArray();
                                        for (IMessage msg : messages) {
                                            jsonArray.add(msg.getMessageBody());
                                        }
//                                        byte[] bytes=jsonArray.toJSONString().getBytes();
//                                        byte[] compareByte=NumberUtils.zlibCompress(bytes);
//                                        System.out.println(bytes.length+"  "+compareByte.length);
                                        newMessage.put("data", jsonArray);
                                        sink.batchAdd(new Message(newMessage));
                                        sink.flush();
                                        return true;
                                    }
                                });
                                ((MessageCache<IMessage>) messageCache).setBatchSize(30);
                                ((MessageCache<IMessage>) messageCache).setAutoFlushTimeGap(300);
                                ((MessageCache<IMessage>) messageCache).setAutoFlushSize(30);
                                messageCache.openAutoFlush();
                            }
                        }
                    }
                    messageCache.addCache(message);

                }
            })
            .start();
    }

    @Test
    public void testConsumer() {
        DataStreamSource.create("tmp", "tmp")
            .fromRocketmq("dipper_test_write_merge", "dipper_group", true, "localhost:9876",null)
            .flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
                @Override public List<JSONObject> flatMap(JSONObject message) throws Exception {
                    JSONArray jsonArray = message.getJSONArray("data");
                    List<JSONObject> messages = new ArrayList<>();
                    for (int i = 0; i < jsonArray.size(); i++) {
                        messages.add(jsonArray.getJSONObject(i));
                    }
                    return messages;
                }
            })
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
            }).forEachMessage(new ForEachMessageFunction() {
                MultiSplitMessageCache messageCache = null;
                RocketMQSink sink = new RocketMQSink("localhost:9876", "dipper_test_write_merge");

                @Override public void foreach(IMessage message, AbstractContext context) {
                    if (messageCache == null) {
                        synchronized (this) {
                            if (messageCache == null) {
                                sink.init();
                                messageCache = new MultiSplitMessageCache(new IMessageFlushCallBack<IMessage>() {

                                    @Override public boolean flushMessage(List<IMessage> messages) {

                                        JSONObject newMessage = new JSONObject();
                                        JSONArray jsonArray = new JSONArray();
                                        for (IMessage msg : messages) {
                                            jsonArray.add(msg.getMessageBody());
                                        }
//                                        byte[] bytes=jsonArray.toJSONString().getBytes();
//                                        byte[] compareByte=NumberUtils.zlibCompress(bytes);
//                                        System.out.println(bytes.length+"  "+compareByte.length);
                                        newMessage.put("data", jsonArray);
                                        sink.batchAdd(new Message(newMessage));
                                        sink.flush();
                                        return true;
                                    }
                                });
                                ((MessageCache<IMessage>) messageCache).setAutoFlushTimeGap(300);
                                ((MessageCache<IMessage>) messageCache).setAutoFlushSize(100);
                                messageCache.openAutoFlush();
                            }
                        }
                    }
                    messageCache.addCache(message);

                }
            })
            .start();
    }
}
