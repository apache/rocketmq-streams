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

package org.apache.rocketmq.streams;

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.junit.Test;

public class RocketMQChannelTest extends AbstractChannelTest {
    private static final String WINDOW_TASK_PROPERTY_KEY_PREFIX = "dipper.window.shuffle.rocketmq.dispatch.channel.";

    @Test
    public void testSource() throws InterruptedException {
        ISource source = createSource();
        source.start(new IStreamOperator() {
            @Override
            public Object doMessage(IMessage iMessage, AbstractContext context) {
                System.out.println(iMessage.getMessageBody().toJSONString());
                return null;
            }
        });

        Thread.sleep(100000000l);
    }

    @Test
    public void testSink() {
        ISink sink = createSink();
        sink.setBatchSize(1);
        for (int i = 0; i < 1000; i++) {
            sink.batchSave(createMsg());
        }
    }

    @Test
    public void testGetSplit() {
        AbstractSupportShuffleSink sink = (AbstractSupportShuffleSink)createSink();
        List<ISplit> splits = sink.getSplitList();
        System.out.println(splits.size());
    }

    //    @Test
    //    public void testCreateChannel() throws InterruptedException {
    //        RocketMQChannelBuilder builder = new RocketMQChannelBuilder();
    //        IChannel consumer = builder.createChannel(getWindowNameSpace(),getWindowName(), createChannelProperties(), null);
    //        IChannel producer = builder.createChannel(getWindowNameSpace(),getWindowName(), createChannelProperties(), null);
    //        consumer.start(new IMessageProcssor() {
    //            @Override
    //            public Object doMessage(IMessage message, AbstractContext context) {
    //                System.out.println(message.getMessageBody().toJSONString());
    //                return null;
    //            }
    //        });((AbstractBatchMessageChannel)producer).getQueueList();
    //
    //        List<ChannelQueue> queueList = ((AbstractBatchMessageChannel)producer).getQueueList();
    //        for (int i=0; i<100000; i++) {
    //
    //            producer.batchSave(createMsg(queueList.get(0)));
    //        }
    //        producer.flush();
    //
    //        Thread.sleep(10000000l);
    //    }

    public List<IMessage> createMsg() {
        JSONObject obj = new JSONObject();
        obj.put("test", "11111");
        obj.put("time", DateUtil.format(new Date()));
        IMessage message = new Message(obj);
        List<IMessage> msgs = new ArrayList<>();
        msgs.add(message);
        return msgs;
    }

    protected Properties createChannelProperties() {
        Properties properties = new Properties();
        Iterator<Map.Entry<Object, Object>> it = ComponentCreator.getProperties().entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<Object, Object> entry = it.next();
            String key = (String)entry.getKey();
            String value = (String)entry.getValue();
            if (key.startsWith(WINDOW_TASK_PROPERTY_KEY_PREFIX)) {
                String channelKey = key.replace(WINDOW_TASK_PROPERTY_KEY_PREFIX, "");
                properties.put(channelKey, value);
            }

        }
        String dynamicProperty = properties.getProperty("dynamic.property");
        if (dynamicProperty != null) {
            String namespace = this.getWindowNameSpace();
            String name = this.getWindowName();
            String startTime = this.getStartTime();
            String endTime = this.getEndTime();
            String startTimeStr = startTime.replace("-", "").replace(" ", "").replace(":", "");
            String endTimeStr = endTime.replace("-", "").replace(" ", "").replace(":", "");
            String dynamicPropertyValue = MapKeyUtil.createKeyBySign("_", namespace, name, startTimeStr + "",
                endTimeStr + "");
            dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_");
            String[] mutilPropertys = dynamicProperty.split(",");
            String groupName = MapKeyUtil.createKeyBySign("_", namespace, name).replaceAll("\\.", "_");
            for (String properyKey : mutilPropertys) {
                if (properyKey.equals("group")) {
                    properties.put(properyKey, groupName);
                } else {
                    properties.put(properyKey, dynamicPropertyValue);
                }

            }
        }
        return properties;
    }

    public String getWindowNameSpace() {
        return "windowNameSpace";
    }

    public String getWindowName() {
        return "windowName";
    }

    public String getStartTime() {
        return "startTime";
    }

    public String getEndTime() {
        return "endTime";
    }

    @Override
    protected ISource createSource() {
        RocketMQChannelBuilder builder = new RocketMQChannelBuilder();
        ISource rocketMQSource = builder.createSource(getWindowNameSpace(), getWindowName(), createChannelProperties(), null);
        //        RocketMQSource rocketMQSource = new RocketMQSource();
        //        rocketMQSource.setTopic("TOPIC_DIPPER_WINDOW_STATISTICS");
        //        rocketMQSource.setTags("test");
        //        rocketMQSource.setAccessKey();
        return rocketMQSource;
    }

    @Override
    protected ISink createSink() {
        RocketMQChannelBuilder builder = new RocketMQChannelBuilder();
        ISink sink = builder.createSink(getWindowNameSpace(), getWindowName(), createChannelProperties(), null);
        return sink;
    }

}
