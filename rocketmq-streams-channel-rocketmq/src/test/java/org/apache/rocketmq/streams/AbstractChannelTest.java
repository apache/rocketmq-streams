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
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.context.Message;
import org.junit.Test;

public abstract class AbstractChannelTest {

    @Test
    public void testChannel() throws InterruptedException {
        ISource channel = createSource();
        channel.setGroupName("CID_YUNDUN_SOC_DIPPER_TEST");
        channel.setMaxThread(1);
        channel.start((message, context) -> {
            //System.out.println(message.getMessageBody().getString(IChannel.OFFSET)+"-"+message.getMessageBody()
            // .getString(IChannel.QUEUE_ID)+"-"+message.getMessageBody().getString(IChannel.IS_BATCH)+"-"+Thread
            // .currentThread().getId());
            System.out.println(message.getMessageBody());
            return message;
        });
        while (true) {
            Thread.sleep(1000L);
        }
    }

    protected abstract ISource createSource();

    protected abstract ISink createSink();

    @Test
    public void testOutput() {
        ISink channel = createSink();
        JSONObject message = new JSONObject();
        message.put("name", "chris");
        message.put("age", 18);
        channel.batchAdd(new Message(message));
        channel.flush();
    }
}
