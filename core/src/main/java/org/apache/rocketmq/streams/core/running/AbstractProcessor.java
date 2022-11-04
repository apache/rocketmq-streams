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


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractProcessor<T> implements Processor<T> {
    private final List<Processor<T>> children = new ArrayList<>();
    protected StreamContext<T> context;

    @Override
    public void addChild(Processor<T> processor) {
        children.add(processor);
    }

    @Override
    public void preProcess(StreamContext<T> context) throws Throwable {
        this.context = context;
        this.context.init(getChildren());
    }

    protected List<Processor<T>> getChildren() {
        return Collections.unmodifiableList(children);
    }

    protected StateStore waitStateReplay() throws Throwable {
        MessageQueue sourceTopicQueue = new MessageQueue(getSourceTopic(), getSourceBrokerName(), getSourceQueueId());

        StateStore stateStore = context.getStateStore();
        stateStore.waitIfNotReady(sourceTopicQueue);
        return stateStore;
    }



    @SuppressWarnings("unchecked")
    protected <KEY> Data<KEY, T> convert(Data<?, ?> data) {
        return (Data<KEY, T>) new Data<>(data.getKey(), data.getValue(), data.getTimestamp());
    }

    @Override
    public void close() throws Exception {

    }

    protected String getSourceBrokerName() {
        String sourceTopicQueue = context.getMessageFromWhichSourceTopicQueue();
        String[] split = Utils.split(sourceTopicQueue);
        return split[0];
    }

    protected String getSourceTopic() {
        String sourceTopicQueue = context.getMessageFromWhichSourceTopicQueue();
        String[] split = Utils.split(sourceTopicQueue);
        return split[1];
    }

    protected Integer getSourceQueueId() {
        String sourceTopicQueue = context.getMessageFromWhichSourceTopicQueue();
        String[] split = Utils.split(sourceTopicQueue);
        return Integer.parseInt(split[2]);
    }
}
