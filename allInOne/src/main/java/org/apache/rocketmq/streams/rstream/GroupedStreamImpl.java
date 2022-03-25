package org.apache.rocketmq.streams.rstream;
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
import org.apache.rocketmq.streams.function.supplier.AggregateActionSupplier;
import org.apache.rocketmq.streams.state.DefaultStore;
import org.apache.rocketmq.streams.state.RocksDBStore;
import org.apache.rocketmq.streams.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.topology.virtual.StatefulProcessorNode;

import static org.apache.rocketmq.streams.OperatorNameMaker.COUNT_PREFIX;

public class GroupedStreamImpl<K, V> implements GroupedStream<K, V> {
    private final Pipeline pipeline;
    private final GraphNode parent;
    private boolean shuffleNode;


    public GroupedStreamImpl(Pipeline pipeline, GraphNode parent) {
        this.pipeline = pipeline;
        this.parent = parent;
    }

    public GroupedStreamImpl(Pipeline pipeline, GraphNode parent, boolean shuffleNode) {
        this.pipeline = pipeline;
        this.parent = parent;
        this.shuffleNode = shuffleNode;
    }

    @Override
    public GroupedStream<K, Long> count() {
        String name = OperatorNameMaker.makeName(COUNT_PREFIX);

        RocksDBStore<K, Long> rocksDBStore = new RocksDBStore<>();
        DefaultStore<K, Long> store = new DefaultStore<>(rocksDBStore);

        AggregateActionSupplier<K, V, Long> supplier = new AggregateActionSupplier<>(name, parent.getName(), store,
                                                                                        () -> 0L, (K key, V value, Long agg) -> agg + 1L);

        GraphNode graphNode;
        if (shuffleNode) {
            //todo  这个supplier提供出去的processor需要包含状态
            graphNode = new StatefulProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addVirtual(graphNode, parent);
    }

    @Override
    public RStream<K, V> toRStream() {
        return new RStreamImpl<>(this.pipeline, parent);
    }
}
