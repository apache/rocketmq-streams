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

import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.function.supplier.AccumulatorSupplier;
import org.apache.rocketmq.streams.core.function.supplier.AddTagSupplier;
import org.apache.rocketmq.streams.core.function.supplier.AggregateSupplier;
import org.apache.rocketmq.streams.core.function.supplier.FilterSupplier;
import org.apache.rocketmq.streams.core.function.supplier.SumAggregate;
import org.apache.rocketmq.streams.core.function.supplier.ValueChangeSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;

import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.FILTER_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.GROUPBY_COUNT_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.GROUPED_STREAM_AGGREGATE_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.MAP_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.MAX_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.MIN_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.SUM_PREFIX;
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

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), () -> 0, (K key, V value, Integer agg) -> agg + 1);

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

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), () -> 0, (K key, V value, Integer agg) -> agg + 1);

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public GroupedStream<K, V> min(SelectAction<? extends Number, V> selectAction) {
        String name = OperatorNameMaker.makeName(MIN_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), () -> null, (AggregateAction<K, V, V>) (key, value, accumulator) -> {
            Number number = selectAction.select(value);
            if (accumulator == null) {
                return value;
            } else {
                Number storedMin = selectAction.select(accumulator);
                double newValue = number.doubleValue();
                double oldValue = storedMin.doubleValue();

                if (newValue < oldValue) {
                    return value;
                } else {
                    return accumulator;
                }
            }
        });

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public GroupedStream<K, V> max(SelectAction<? extends Number, V> selectAction) {
        String name = OperatorNameMaker.makeName(MAX_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), () -> null, (AggregateAction<K, V, V>) (key, value, accumulator) -> {
            Number number = selectAction.select(value);
            if (accumulator == null) {
                return value;
            } else {
                Number storedMin = selectAction.select(accumulator);
                double newValue = number.doubleValue();
                double oldValue = storedMin.doubleValue();

                if (newValue > oldValue) {
                    return value;
                } else {
                    return accumulator;
                }
            }
        });

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public GroupedStream<K, ? extends Number> sum(SelectAction<? extends Number, V> selectAction) {
        String name = OperatorNameMaker.makeName(SUM_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new AggregateSupplier<>(name, parent.getName(), () -> null, new SumAggregate<>(selectAction));

        GraphNode graphNode;
        if (this.parent.shuffleNode()) {
            graphNode = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            graphNode = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public GroupedStream<K, V> filter(FilterAction<V> predictor) {
        String name = OperatorNameMaker.makeName(FILTER_PREFIX, pipeline.getJobId());

        FilterSupplier<V> supplier = new FilterSupplier<>(predictor);
        GraphNode graphNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public <OUT> GroupedStream<K, OUT> map(ValueMapperAction<V, OUT> mapperAction) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX, pipeline.getJobId());

        ValueChangeSupplier<V, OUT> supplier = new ValueChangeSupplier<>(mapperAction);
        GraphNode graphNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return this.pipeline.addGroupedStreamVirtualNode(graphNode, parent);
    }

    @Override
    public <OUT> GroupedStream<K, OUT> aggregate(Accumulator<V, OUT> accumulator) {
        String name = OperatorNameMaker.makeName(GROUPED_STREAM_AGGREGATE_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new AccumulatorSupplier<>(name, parent.getName(), value -> value, accumulator);

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
