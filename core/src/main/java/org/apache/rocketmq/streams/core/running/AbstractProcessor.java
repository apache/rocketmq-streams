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


import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.state.StateStore;

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

    protected StateStore waitStateReplay() throws Throwable {
        MessageExt originData = context.getOriginData();
        MessageQueue sourceTopicQueue = new MessageQueue(originData.getTopic(), originData.getBrokerName(), originData.getQueueId());

        StateStore stateStore = context.getStateStore();
        stateStore.waitIfNotReady(sourceTopicQueue);
        return stateStore;
    }

    protected List<Processor<T>> getChildren() {
        return Collections.unmodifiableList(children);
    }

    @SuppressWarnings("unchecked")
    protected <KEY> Data<KEY, T> convert(Data<?, ?> data) {
        return (Data<KEY, T>) new Data<>(data.getKey(), data.getValue());
    }

    @Override
    public void close() throws Exception {

    }
}
