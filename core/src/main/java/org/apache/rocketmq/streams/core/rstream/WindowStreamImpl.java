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
import org.apache.rocketmq.streams.core.function.supplier.WindowAggregateSupplier;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;

import static org.apache.rocketmq.streams.core.OperatorNameMaker.WINDOW_COUNT_PREFIX;

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
    public WindowStream<K, Long> count() {
        String name = OperatorNameMaker.makeName(WINDOW_COUNT_PREFIX);

        WindowAggregateSupplier<K, V, Long> supplier = new WindowAggregateSupplier<>(name, parent.getName(), windowInfo, () -> 0L, (K key, V value, Long agg) -> agg + 1L);

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
    public void aggregate() {

    }

    @Override
    public RStream<V> toRStream() {
        return new RStreamImpl<>(this.pipeline, parent);
    }
}
