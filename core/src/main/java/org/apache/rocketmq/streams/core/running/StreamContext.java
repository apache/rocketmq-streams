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

import java.util.HashMap;
import java.util.List;

public interface StreamContext<V> {
    void init(List<Processor<V>> childrenProcessors);

    StateStore getStateStore();

    long getDataTime();

    <K> K getKey();

    long getWatermark();

    DefaultMQProducer getDefaultMQProducer();

    String getMessageFromWhichSourceTopicQueue();

//    void passWatermark(long watermark) throws Throwable;

    <K> void forward(Data<K, V> data) throws Throwable;
}
