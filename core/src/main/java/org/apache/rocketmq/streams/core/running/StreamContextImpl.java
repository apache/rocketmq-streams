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

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.core.window.fire.IdleWindowScaner;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 1、可以获得当前processor；
 * 2、可以获得下一个执行节点
 * 3、可获得动态的运行时信息，例如正在处理的数据来自那个topic，MQ，偏移量多少；
 */
public class StreamContextImpl<V> implements StreamContext<V> {
    private static final Logger logger = LoggerFactory.getLogger(StreamContextImpl.class);

    private final Properties properties;
    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final StateStore stateStore;
    private final String messageFromWhichSourceTopicQueue;
    private final IdleWindowScaner idleWindowScaner;

    private Object key;
    private long dataTime;
    private Properties header = new Properties();

    private final List<Processor<V>> childList = new ArrayList<>();

    StreamContextImpl(Properties properties,
                      DefaultMQProducer producer,
                      DefaultMQAdminExt mqAdmin,
                      StateStore stateStore,
                      String messageFromWhichSourceTopicQueue,
                      IdleWindowScaner idleWindowScaner) {
        this.properties = properties;
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.stateStore = stateStore;
        this.messageFromWhichSourceTopicQueue = messageFromWhichSourceTopicQueue;
        this.idleWindowScaner = idleWindowScaner;
    }

    @Override
    public void init(List<Processor<V>> childrenProcessors) {
        this.childList.clear();
        if (childrenProcessors != null) {
            this.childList.addAll(childrenProcessors);
        }
    }

    @Override
    public StateStore getStateStore() {
        return this.stateStore;
    }

    @Override
    public DefaultMQProducer getDefaultMQProducer() {
        return producer;
    }


    public String getSourceBrokerName() {
        String[] split = Utils.split(messageFromWhichSourceTopicQueue);
        return split[0];
    }

    public String getSourceTopic() {
        String[] split = Utils.split(messageFromWhichSourceTopicQueue);
        return split[1];
    }

    public Integer getSourceQueueId() {
        String[] split = Utils.split(messageFromWhichSourceTopicQueue);
        return Integer.parseInt(split[2]);
    }

    @Override
    public long getDataTime() {
        return this.dataTime;
    }


    @Override
    @SuppressWarnings("unchecked")
    public <K> K getKey() {
        return (K) key;
    }

    <K> void setKey(K key) {
        this.key = key;
    }


    @Override
    public Properties getUserProperties() {
        Properties result = new Properties();
        result.putAll(this.properties);

        return result;
    }


    @Override
    public Properties getHeader() {
        Properties result = new Properties();
        result.putAll(this.header);

        return result;
    }

    @Override
    public IdleWindowScaner getDefaultWindowScaner() {
        return this.idleWindowScaner;
    }


    @Override
    public StreamContext<V> copy() {
        StreamContextImpl<V> streamContext = new StreamContextImpl<>(this.properties,
                this.producer,
                this.mqAdmin,
                this.stateStore,
                this.messageFromWhichSourceTopicQueue,
                this.idleWindowScaner);
        streamContext.key = this.key;
        streamContext.dataTime = this.dataTime;
        streamContext.header = new Properties(this.header);
        streamContext.childList.addAll(this.childList);

        return streamContext;
    }

    @Override
    public <K> void forward(Data<K, V> data) throws Throwable {
        this.key = data.getKey();

        if (data.getTimestamp() != null) {
            this.dataTime = data.getTimestamp();
        }

        this.header = data.getHeader();

        List<Processor<V>> store = new ArrayList<>(childList);

        for (Processor<V> processor : childList) {

            try {
                processor.preProcess(this);
                processor.process(data.getValue());
            } finally {
                this.childList.clear();
                this.childList.addAll(store);
            }
        }
    }


}
