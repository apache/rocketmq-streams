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
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSupportShuffleSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.utils.DateUtil;
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
        AbstractSupportShuffleSink sink = (AbstractSupportShuffleSink) createSink();
        List<ISplit<?, ?>> splits = sink.getSplitList();
        System.out.println(splits.size());
    }

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
        return new Properties();
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
        return builder.createSource(getWindowNameSpace(), getWindowName(), createChannelProperties(), null);
    }

    @Override
    protected ISink createSink() {
        RocketMQChannelBuilder builder = new RocketMQChannelBuilder();
        return builder.createSink(getWindowNameSpace(), getWindowName(), createChannelProperties(), null);
    }

}
