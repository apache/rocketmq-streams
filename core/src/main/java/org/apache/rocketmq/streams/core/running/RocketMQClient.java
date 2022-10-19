package org.apache.rocketmq.streams.core.running;
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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.Set;
import java.util.UUID;

import static org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData.SUB_ALL;

public class RocketMQClient {
    private final String nameSrvAddr;

    public RocketMQClient(String nameSrvAddr) {
        this.nameSrvAddr = nameSrvAddr;
    }

    public DefaultLitePullConsumer pullConsumer(String groupName, Set<String> topics) throws MQClientException {
        DefaultLitePullConsumer pullConsumer = new DefaultLitePullConsumer(groupName);
        pullConsumer.setNamesrvAddr(nameSrvAddr);
        pullConsumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
        pullConsumer.setAutoCommit(false);


        for (String topic : topics) {
            pullConsumer.subscribe(topic, SUB_ALL);
        }

        return pullConsumer;
    }

    public DefaultMQProducer producer(String groupName) {
        DefaultMQProducer producer = new DefaultMQProducer(groupName);
        producer.setNamesrvAddr(nameSrvAddr);
        return producer;
    }

    public DefaultMQAdminExt getMQAdmin() {
        DefaultMQAdminExt mqAdminExt = new DefaultMQAdminExt();
        mqAdminExt.setInstanceName(UUID.randomUUID().toString());
        mqAdminExt.setNamesrvAddr(nameSrvAddr);
        return mqAdminExt;
    }
}
