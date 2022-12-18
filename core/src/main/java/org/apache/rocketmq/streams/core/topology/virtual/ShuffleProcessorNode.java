package org.apache.rocketmq.streams.core.topology.virtual;
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

import org.apache.rocketmq.streams.core.util.OperatorNameMaker;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.supplier.SinkSupplier;
import org.apache.rocketmq.streams.core.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.serialization.deImpl.KVJsonDeserializer;
import org.apache.rocketmq.streams.core.serialization.serImpl.KVJsonSerializer;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;

import java.util.List;
import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.SHUFFLE_SINK_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.SHUFFLE_SOURCE_PREFIX;

public class ShuffleProcessorNode<T> extends ProcessorNode<T> {


    public ShuffleProcessorNode(String name, String parentName, Supplier<Processor<T>> supplier) {
        super(name, parentName, supplier);
    }

    @Override
    public void addRealNode(TopologyBuilder builder) {
        String topicName = name + Constant.SHUFFLE_TOPIC_SUFFIX;
        //join的时候需要一个相同的topicName

        String shuffleSinkName = OperatorNameMaker.makeName(SHUFFLE_SINK_PREFIX);
        for (String parentName : parentNames) {
            builder.addRealSink(shuffleSinkName, parentName, topicName, new SinkSupplier<>(topicName, new KVJsonSerializer<>()));
        }
        String shuffleSourceName = OperatorNameMaker.makeName(SHUFFLE_SOURCE_PREFIX);
        builder.addRealSource(shuffleSourceName, topicName, new SourceSupplier<>(topicName, new KVJsonDeserializer<>()));

        builder.addRealNode(name, shuffleSourceName, supplier);
    }
}
