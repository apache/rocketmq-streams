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
package org.apache.rocketmq.streams.core.rstream;

import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.function.accumulator.AvgAccumulator;
import org.apache.rocketmq.streams.core.function.accumulator.CountAccumulator;
import org.apache.rocketmq.streams.core.function.supplier.FilterSupplier;
import org.apache.rocketmq.streams.core.function.supplier.SinkSupplier;
import org.apache.rocketmq.streams.core.function.supplier.ValueChangeSupplier;
import org.apache.rocketmq.streams.core.function.supplier.WindowAccumulatorSupplier;
import org.apache.rocketmq.streams.core.function.supplier.WindowAggregateSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.SinkGraphNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;
import org.apache.rocketmq.streams.core.window.WindowInfo;

import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.COUNT_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.FILTER_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.MAP_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.SINK_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.WINDOW_AVG_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.AGGREGATE_PREFIX;


public class WindowStreamImpl<K, V> implements WindowStream<K, V> {
    private final Pipeline pipeline;
    private final GraphNode parent;
    private final WindowInfo windowInfo;

    public WindowStreamImpl(Pipeline pipeline, GraphNode parent, WindowInfo windowInfo) {
        this.pipeline = pipeline;
        this.parent = parent;
        this.windowInfo = windowInfo;
    }

    @Override
    public WindowStream<K, Integer> count() {
        String name = OperatorNameMaker.makeName(COUNT_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new WindowAccumulatorSupplier<>(name, windowInfo, value -> value, new CountAccumulator<>());

        //是否需要分组计算
        ProcessorNode<V> node;
        if (this.parent.shuffleNode()) {
            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            node = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public WindowStream<K, Double> avg() {
        String name = OperatorNameMaker.makeName(WINDOW_AVG_PREFIX, pipeline.getJobId());
        Supplier<Processor<V>> supplier = new WindowAccumulatorSupplier<>(name, windowInfo, value -> value, new AvgAccumulator<>());

        //是否需要分组计算
        ProcessorNode<V> node;
        if (this.parent.shuffleNode()) {
            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            node = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public WindowStream<K, V> filter(FilterAction<V> predictor) {
        String name = OperatorNameMaker.makeName(FILTER_PREFIX, pipeline.getJobId());

        FilterSupplier<V> supplier = new FilterSupplier<>(predictor);
        GraphNode graphNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return this.pipeline.addWindowStreamVirtualNode(graphNode, parent, windowInfo);
    }

    @Override
    public <OUT> WindowStream<K, OUT> map(ValueMapperAction<V, OUT> mapperAction) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX, pipeline.getJobId());

        ValueChangeSupplier<V, OUT> supplier = new ValueChangeSupplier<>(mapperAction);
        GraphNode graphNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return this.pipeline.addWindowStreamVirtualNode(graphNode, parent, windowInfo);
    }

    @Override
    public <OUT> WindowStream<K, OUT> aggregate(AggregateAction<K, V, OUT> aggregateAction) {
        String name = OperatorNameMaker.makeName(AGGREGATE_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new WindowAggregateSupplier<>(name, windowInfo, () -> null, aggregateAction);

        //是否需要分组计算
        ProcessorNode<V> node;

        if (this.parent.shuffleNode()) {
            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            node = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public <OUT> WindowStream<K, OUT> aggregate(Accumulator<V, OUT> accumulator) {
        String name = OperatorNameMaker.makeName(AGGREGATE_PREFIX, pipeline.getJobId());

        Supplier<Processor<V>> supplier = new WindowAccumulatorSupplier<>(name, windowInfo, value -> value, accumulator);

        //是否需要分组计算
        ProcessorNode<V> node;

        if (this.parent.shuffleNode()) {
            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
        } else {
            node = new ProcessorNode<>(name, parent.getName(), supplier);
        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public RStream<V> toRStream() {
        return new RStreamImpl<>(this.pipeline, parent);
    }

    @Override
    public void sink(String topicName, KeyValueSerializer<K, V> serializer) {
        String name = OperatorNameMaker.makeName(SINK_PREFIX, pipeline.getJobId());

        SinkSupplier<K, V> sinkSupplier = new SinkSupplier<>(topicName, serializer);
        GraphNode sinkGraphNode = new SinkGraphNode<>(name, parent.getName(), topicName, sinkSupplier);

        pipeline.addVirtualSink(sinkGraphNode, parent);
    }
}
