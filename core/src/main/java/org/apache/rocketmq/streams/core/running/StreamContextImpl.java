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
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.ArrayList;
import java.util.List;

/**
 * 1、可以获得当前processor；
 * 2、可以获得下一个执行节点
 * 3、可获得动态的运行时信息，例如正在处理的数据来自那个topic，MQ，偏移量多少；
 */
public class StreamContextImpl<T> implements StreamContext<T> {

    private final DefaultMQProducer producer;
    private final DefaultMQAdminExt mqAdmin;
    private final StateStore stateStore;
    private final String messageFromWhichSourceTopicQueue;

    private Object key;
    private long dataTime;

    private long watermark;

    private final List<Processor<T>> childList = new ArrayList<>();

    StreamContextImpl(DefaultMQProducer producer, DefaultMQAdminExt mqAdmin, StateStore stateStore, String messageFromWhichSourceTopicQueue) {
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.stateStore = stateStore;
        this.messageFromWhichSourceTopicQueue = messageFromWhichSourceTopicQueue;
    }

    void setWatermark(long watermark) {
        this.watermark = watermark;
    }

    @Override
    public void init(List<Processor<T>> childrenProcessors) {
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

    public String getMessageFromWhichSourceTopicQueue() {
        return messageFromWhichSourceTopicQueue;
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

    @Override
    public long getWatermark() {
        return watermark;
    }

    @Override
    public <K> void forward(Data<K, T> data) throws Throwable {
        this.key = data.getKey();
        this.dataTime = data.getTimestamp();

        List<Processor<T>> store = new ArrayList<>(childList);

        for (Processor<T> processor : childList) {

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
