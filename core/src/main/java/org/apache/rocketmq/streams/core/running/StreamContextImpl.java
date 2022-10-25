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
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.ArrayList;
import java.util.HashMap;
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
    private final MessageExt messageExt;

    private final List<Processor<T>> childList = new ArrayList<>();
    private Data<?, T> data;
    private final HashMap<String, Object> additional = new HashMap<>();

    public StreamContextImpl(DefaultMQProducer producer, DefaultMQAdminExt mqAdmin, StateStore stateStore, MessageExt messageExt) {
        this.producer = producer;
        this.mqAdmin = mqAdmin;
        this.stateStore = stateStore;
        this.messageExt = messageExt;
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
    public <K> void setData(Data<K, T> data) {
        this.data = data;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K> Data<K, T> getData() {
        return (Data<K, T>) this.data;
    }

    @Override
    public DefaultMQProducer getDefaultMQProducer() {
        return producer;
    }

    @Override
    public HashMap<String, Object> getAdditional() {
        return additional;
    }

    @Override
    public MessageExt getOriginData() {
        return this.messageExt;
    }

    @Override
    public <K> void forward(Data<K, T> data) throws Throwable {
        this.data = data;
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
