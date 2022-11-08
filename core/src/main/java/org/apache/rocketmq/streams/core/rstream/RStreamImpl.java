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


import org.apache.rocketmq.streams.core.OperatorNameMaker;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.ForeachAction;
import org.apache.rocketmq.streams.core.function.KeySelectAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.function.supplier.FilterSupplier;
import org.apache.rocketmq.streams.core.function.supplier.ForeachSupplier;
import org.apache.rocketmq.streams.core.function.supplier.KeySelectSupplier;
import org.apache.rocketmq.streams.core.function.supplier.PrintSupplier;
import org.apache.rocketmq.streams.core.function.supplier.SinkSupplier;
import org.apache.rocketmq.streams.core.function.supplier.TimestampSelectorSupplier;
import org.apache.rocketmq.streams.core.function.supplier.ValueChangeSupplier;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.SinkGraphNode;

import static org.apache.rocketmq.streams.core.OperatorNameMaker.FILTER_PREFIX;
import static org.apache.rocketmq.streams.core.OperatorNameMaker.FOR_EACH_PREFIX;
import static org.apache.rocketmq.streams.core.OperatorNameMaker.GROUPBY_PREFIX;
import static org.apache.rocketmq.streams.core.OperatorNameMaker.MAP_PREFIX;
import static org.apache.rocketmq.streams.core.OperatorNameMaker.SINK_PREFIX;

public class RStreamImpl<T> implements RStream<T> {
    private final Pipeline pipeline;
    private final GraphNode parent;

    public RStreamImpl(Pipeline pipeline, GraphNode parent) {
        this.pipeline = pipeline;
        this.parent = parent;
    }

    @Override
    public RStream<T> selectTimestamp(ValueMapperAction<T, Long> timestampSelector) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX);

        TimestampSelectorSupplier<T> supplier = new TimestampSelectorSupplier<>(timestampSelector);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addRStreamVirtualNode(processorNode, parent);
    }

    @Override
    public <O> RStream<O> map(ValueMapperAction<T, O> mapperAction) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX);

        ValueChangeSupplier<T, O> supplier = new ValueChangeSupplier<>(mapperAction);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addRStreamVirtualNode(processorNode, parent);
    }

    @Override
    public <VR> RStream<T> flatMapValues(ValueMapperAction<? extends T, ? extends Iterable<? extends VR>> mapper) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX);

        ValueChangeSupplier<? extends T, ? extends Iterable<? extends VR>> supplier = new ValueChangeSupplier<>(mapper);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addRStreamVirtualNode(processorNode, parent);
    }

    @Override
    public RStream<T> filter(FilterAction<T> predictor) {
        String name = OperatorNameMaker.makeName(FILTER_PREFIX);

        FilterSupplier<T> supplier = new FilterSupplier<>(predictor);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addRStreamVirtualNode(processorNode, parent);
    }

    @Override
    public <K> GroupedStream<K, T> keyBy(KeySelectAction<K, T> keySelectAction) {
        String name = OperatorNameMaker.makeName(GROUPBY_PREFIX);

        KeySelectSupplier<K, T> keySelectSupplier = new KeySelectSupplier<>(keySelectAction);

        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), true, keySelectSupplier);

        return pipeline.addGroupedStreamVirtualNode(processorNode, parent);
    }

    @Override
    public void print() {
        String name = OperatorNameMaker.makeName(SINK_PREFIX);

        PrintSupplier<T> printSupplier = new PrintSupplier<>();
        GraphNode sinkGraphNode = new SinkGraphNode<>(name, parent.getName(), null, printSupplier);

        pipeline.addVirtualSink(sinkGraphNode, parent);
    }

    @Override
    public RStream<T> foreach(ForeachAction<T> foreachAction) {
        String name = OperatorNameMaker.makeName(FOR_EACH_PREFIX);

        ForeachSupplier<T> supplier = new ForeachSupplier<T>(foreachAction);

        ProcessorNode<T> node = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addRStreamVirtualNode(node, parent);
    }

    @Override
    public RStream<T> join(RStream<T> rightStream) {


        return null;
    }

    @Override
    public <K> void sink(String topicName, KeyValueSerializer<K, T> serializer) {
        String name = OperatorNameMaker.makeName(SINK_PREFIX);

        SinkSupplier<K, T> sinkSupplier = new SinkSupplier<>(topicName, serializer);
        GraphNode sinkGraphNode = new SinkGraphNode<>(name, parent.getName(), topicName, sinkSupplier);

        pipeline.addVirtualSink(sinkGraphNode, parent);
    }

}
