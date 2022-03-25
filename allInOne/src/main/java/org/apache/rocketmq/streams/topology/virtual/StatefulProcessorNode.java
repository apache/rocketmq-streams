package org.apache.rocketmq.streams.topology.virtual;
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

import org.apache.rocketmq.streams.OperatorNameMaker;
import org.apache.rocketmq.streams.function.supplier.SinkSupplier;
import org.apache.rocketmq.streams.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.state.DefaultStore;
import org.apache.rocketmq.streams.state.RocksDBStore;
import org.apache.rocketmq.streams.topology.TopologyBuilder;

import java.util.function.Supplier;

import static org.apache.rocketmq.streams.OperatorNameMaker.SHUFFLE_SINK_PREFIX;
import static org.apache.rocketmq.streams.OperatorNameMaker.SHUFFLE_SOURCE_PREFIX;

public class StatefulProcessorNode<K, V, OK, OV> extends ProcessorNode<K, V, OK, OV> {
    private static final String SUFFIX = "-shuffle";

    public StatefulProcessorNode(String name, String parentName, Supplier<? extends Processor<K, V, OK, OV>> supplier) {
        super(name, parentName, supplier);
    }

    @Override
    public void addRealNode(TopologyBuilder builder) {
        String topicName = name + SUFFIX;

        String shuffleSinkName = OperatorNameMaker.makeName(SHUFFLE_SINK_PREFIX);
        builder.addRealSink(shuffleSinkName, parentName, topicName, new SinkSupplier<>(topicName));

        String shuffleSourceName = OperatorNameMaker.makeName(SHUFFLE_SOURCE_PREFIX);
        builder.addRealSource(shuffleSourceName, topicName, new SourceSupplier<>(topicName));

        //构造一个状态存储
//        RocksDBStore<K,V> rocksDBStore = new RocksDBStore<>();
//        DefaultStore<K,V> store = new DefaultStore<>(rocksDBStore);
//        builder.addStatefulRealNode(name, parentName, store, supplier);
        builder.addRealNode(name, shuffleSourceName, supplier);
    }
}
