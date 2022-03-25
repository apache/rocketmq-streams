package org.apache.rocketmq.streams.running;
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

import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.streams.metadata.Data;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * 1、可以获得当前processor；
 * 2、可以获得下一个执行节点
 * 3、可获得动态的运行时信息，例如正在处理的数据来自那个topic，MQ，偏移量多少；
 */
public class StreamContextImpl implements StreamContext {
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final MessageExt messageExt;
    private final List<Processor<?, ?, ?, ?>> childList = new ArrayList<>();


    public StreamContextImpl(DefaultMQProducer producer, DefaultMQAdminExt mqAdmin, MessageExt messageExt) {
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.messageExt = messageExt;
    }

    @Override
    public <K, V, OK, OV> void init(List<Processor<K, V, OK, OV>> childrenProcessors) {
        this.childList.clear();
        if (childrenProcessors != null) {
            this.childList.addAll(childrenProcessors);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public <K, V> void forward(Data<K, V> data) {
        if (childList.size() == 0 && !StringUtils.isEmpty(data.getSinkTopic())) {
            //todo 创建compact topic
            //根据不同key选择不同MessageQueue写入消息；

            String key = String.valueOf(data.getKey());
            String value = data.getValue().toString();
            Message message = new Message(data.getSinkTopic(), "", key, value.getBytes(StandardCharsets.UTF_8));

            try {
                producer.send(message);
            } catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
                //todo 异常体系，哪些可以不必中断线程，哪些是需要中断的？
                e.printStackTrace();
            }

            return;
        }


        List<Processor<?, ?, ?, ?>> store = new ArrayList<>(childList);

        for (Processor<?, ?, ?, ?> processor : childList) {

            try {
                processor.preProcess(this);
                ((Processor<K, V, ?, ?>) processor).process(data);
            } finally {
                this.childList.clear();
                this.childList.addAll(store);
            }
        }

    }


}
