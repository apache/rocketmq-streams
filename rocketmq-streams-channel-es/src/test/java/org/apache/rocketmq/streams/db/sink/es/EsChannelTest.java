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
package org.apache.rocketmq.streams.db.sink.es;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;

import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.es.sink.ESSinkOnlyChannel;
import org.junit.Test;

public class EsChannelTest {
    String host = "es-cn.elasticsearch.xxxxx.com";
    String port = "9200";

    @Test
    public void testEsChannelInsert() {
        JSONObject message = JSONObject.parseObject("{\"data\": \"xxx test message\", \"time\":\"2020-07-25\"}");
        List<IMessage> messageList = new ArrayList<>();
        messageList.add(new Message(message));
        ISink esChannel = createChannel();
        esChannel.batchSave(messageList);
    }

    private ISink createChannel() {
        ESSinkOnlyChannel esChannel = new ESSinkOnlyChannel();
        esChannel.setHost(host);
        esChannel.setPort(port + "");
        esChannel.setEsIndex("es_index_test");
        esChannel.setNeedAuth(true);
        esChannel.setAuthUsername("elastic");
        esChannel.setAuthPassword("Admin123");
        esChannel.init();
        return esChannel;
    }
}
