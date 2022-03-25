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
import org.apache.rocketmq.streams.function.FilterAction;
import org.apache.rocketmq.streams.function.ForeachAction;
import org.apache.rocketmq.streams.function.MapperAction;
import org.apache.rocketmq.streams.function.ValueMapperAction;
import org.apache.rocketmq.streams.function.supplier.FilterActionSupplier;
import org.apache.rocketmq.streams.function.supplier.MapperActionSupplier;
import org.apache.rocketmq.streams.function.supplier.PrintActionSupplier;
import org.apache.rocketmq.streams.function.supplier.SinkSupplier;
import org.apache.rocketmq.streams.function.supplier.ValueActionSupplier;
import org.apache.rocketmq.streams.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.topology.virtual.SinkGraphNode;

import static org.apache.rocketmq.streams.OperatorNameMaker.FILTER_PREFIX;
import static org.apache.rocketmq.streams.OperatorNameMaker.GROUPBY_PREFIX;
import static org.apache.rocketmq.streams.OperatorNameMaker.MAP_PREFIX;
import static org.apache.rocketmq.streams.OperatorNameMaker.SINK_PREFIX;

public class RStreamImpl<K, V> implements RStream<K, V> {
    private final Pipeline pipeline;
    private final GraphNode parent;

    public RStreamImpl(Pipeline pipeline, GraphNode parent) {
        this.pipeline = pipeline;
        this.parent = parent;
    }

    @Override
    public <OV> RStream<K, OV> map(ValueMapperAction<V, OV> mapperAction) {
        String name = OperatorNameMaker.makeName(MAP_PREFIX);

        ValueActionSupplier<K, V, OV> supplier = new ValueActionSupplier<>(mapperAction);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addVirtualNode(processorNode, parent);
    }

    @Override
    public RStream<K, V> filter(FilterAction<K, V> predictor) {
        String name = OperatorNameMaker.makeName(FILTER_PREFIX);

        FilterActionSupplier<K, V> supplier = new FilterActionSupplier<>(predictor);
        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), supplier);

        return pipeline.addVirtualNode(processorNode, parent);
    }

    @Override
    public <R> GroupedStream<R, V> groupBy(MapperAction<K, V, R> mapperAction) {
        String name = OperatorNameMaker.makeName(GROUPBY_PREFIX);

        MapperActionSupplier<K, V, R> mapperActionSupplier = new MapperActionSupplier<>(mapperAction);

        GraphNode processorNode = new ProcessorNode<>(name, parent.getName(), true, mapperActionSupplier);

        return pipeline.addVirtual(processorNode, parent);
    }

    @Override
    public void print() {
        String name = OperatorNameMaker.makeName(SINK_PREFIX);

        PrintActionSupplier<K, V, ?, ?> printActionSupplier = new PrintActionSupplier<>();
        GraphNode sinkGraphNode = new SinkGraphNode<>(name, parent.getName(), null, printActionSupplier);

        pipeline.addVirtualSink(sinkGraphNode, parent);
    }

    @Override
    public void foreach(ForeachAction<K, V> foreachAction) {

    }

    @Override
    public void sink(String topicName) {
        String name = OperatorNameMaker.makeName(SINK_PREFIX);

        SinkSupplier<K, V, ?, ?> sinkSupplier = new SinkSupplier<>(topicName);
        GraphNode sinkGraphNode = new SinkGraphNode<>(name, parent.getName(), topicName, sinkSupplier);

        pipeline.addVirtualSink(sinkGraphNode, parent);
    }
}
