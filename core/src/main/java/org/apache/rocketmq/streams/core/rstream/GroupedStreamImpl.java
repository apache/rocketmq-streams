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

import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.function.accumulator.MinAccumulator;
import org.apache.rocketmq.streams.core.function.supplier.AddTagSupplier;
import org.apache.rocketmq.streams.core.function.accumulator.CountAccumulator;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;
import org.apache.rocketmq.streams.core.function.supplier.AggregateSupplier;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;

import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.GROUPBY_COUNT_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.GROUPBY_MIN_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.GROUPED_STREAM_AGGREGATE_PREFIX;
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
        String name = OperatorNameMaker.makeName(GROUPBY_COUNT_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), value -> value, new CountAccumulator<>());

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public <OUT> GroupedStream<K, Integer> count(SelectAction<OUT, V> selectAction) {
        String name = OperatorNameMaker.makeName(GROUPBY_COUNT_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), selectAction, new CountAccumulator<>());

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public <OUT> GroupedStream<K, V> min(SelectAction<OUT, V> selectAction) {
        String name = OperatorNameMaker.makeName(GROUPBY_MIN_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), selectAction, new MinAccumulator<>());

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public <OUT> GroupedStream<K, V> max(SelectAction<OUT, V> selectAction) {
        return null;
    }

    @Override
    public <OUT> GroupedStream<K, V> sum(SelectAction<OUT, V> selectAction) {
        return null;
    }

    @Override
    public GroupedStream<K, V> filter(FilterAction<V> predictor) {
        return null;
    }

    @Override
    public <OUT> GroupedStream<K, OUT> map(ValueMapperAction<V, OUT> valueMapperAction) {
        return null;
    }

    @Override
    public <OUT> GroupedStream<K, OUT> aggregate(Accumulator<V, OUT> accumulator) {
        String name = OperatorNameMaker.makeName(GROUPED_STREAM_AGGREGATE_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), value -> value, accumulator);

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
        String name = OperatorNameMaker.makeName(WINDOW_ADD_TAG, pipeline.getJobId());

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

    @Override
    public void sink(String topicName, KeyValueSerializer<K, V> serializer) {

    }
}
