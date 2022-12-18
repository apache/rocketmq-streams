package org.apache.rocketmq.streams.core.rstream;
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

import org.apache.rocketmq.streams.core.function.supplier.AddTagSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;
import org.apache.rocketmq.streams.core.function.supplier.AggregateSupplier;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;

import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.COUNT_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.WINDOW_ADD_TAG;

public class GroupedStreamImpl<K, V> implements GroupedStream<K, V> {
    private final Pipeline pipeline;
    private final GraphNode parent;

    public GroupedStreamImpl(Pipeline pipeline, GraphNode parent) {
        this.pipeline = pipeline;
        this.parent = parent;
    }

    @Override
    public GroupedStream<K, Integer> count() {
        String name = OperatorNameMaker.makeName(COUNT_PREFIX);

        AggregateSupplier<K, V, Integer> supplier = new AggregateSupplier<>(name, parent.getName(), () -> 0, (K key, V value, Integer agg) -> agg + 1);

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public WindowStream<K, V> window(WindowInfo windowInfo) {
        //需要在window里面shuffle
        String name = OperatorNameMaker.makeName(WINDOW_ADD_TAG);

        ProcessorNode<V> node;

        if (!this.parent.shuffleNode()) {
            node = new ProcessorNode<>(name, parent.getName(), new AddTagSupplier<>());
        } else if (windowInfo.getJoinStream() != null) {
            node = new ShuffleProcessorNode<>(name, parent.getName(), new AddTagSupplier<>(windowInfo::getJoinStream));
        } else {
            node = new ShuffleProcessorNode<>(name, parent.getName(), new AddTagSupplier<>());
        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public GroupedStream<K, V> addGraphNode(String name, Supplier<Processor<V>> supplier) {
        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            //todo  这个supplier提供出去的processor需要包含状态
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public RStream<V> toRStream() {
        return new RStreamImpl<>(this.pipeline, parent);
    }
}
