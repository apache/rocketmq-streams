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

import org.apache.rocketmq.streams.core.function.supplier.JoinWindowAggregateSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.util.CommonNameMaker;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.supplier.WindowAggregateSupplier;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.topology.virtual.ShuffleProcessorNode;

import java.util.Properties;
import java.util.function.Supplier;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.WINDOW_AGGREGATE_PREFIX;
import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.WINDOW_COUNT_PREFIX;

public class WindowStreamImpl<K, V> implements WindowStream<K, V> {
    private final Pipeline pipeline;
    private final GraphNode parent;
    private final WindowInfo windowInfo;
    private final Properties properties = new Properties();

    public WindowStreamImpl(Pipeline pipeline, GraphNode parent, WindowInfo windowInfo) {
        this.pipeline = pipeline;
        this.parent = parent;
        this.windowInfo = windowInfo;
    }

    @Override
    public WindowStream<K, Integer> count() {
        String name = makeName(WINDOW_COUNT_PREFIX);
        Supplier<Processor<V>> supplier;
        if (windowInfo.getJoinStream() == null) {
            supplier = new WindowAggregateSupplier<>(windowInfo, () -> 0, (K key, V value, Integer agg) -> agg + 1);
        } else {
            supplier = new JoinWindowAggregateSupplier<>(name, parent.getName(), windowInfo, () -> 0, (K key, V value, Integer agg) -> agg + 1);
        }

        //是否需要分组计算
        ProcessorNode<V> node = new ProcessorNode<>(name, parent.getName(), supplier);
//        if (this.parent.shuffleNode()) {
//            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
//        } else {
//            node = new ProcessorNode<>(name, parent.getName(), supplier);
//        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public <OUT> WindowStream<K, V> aggregate(AggregateAction<K, V, OUT> aggregateAction) {
        String name = makeName(WINDOW_AGGREGATE_PREFIX);

        Supplier<Processor<V>> supplier;
        if (windowInfo.getJoinStream() == null) {
            supplier = new WindowAggregateSupplier<>(windowInfo, () -> null, aggregateAction);
        } else {
            supplier = new JoinWindowAggregateSupplier<>(name, parent.getName(), windowInfo, () -> null, aggregateAction);
        }

        //是否需要分组计算
        ProcessorNode<V> node = new ProcessorNode<>(name, parent.getName(), supplier);

//        if (this.parent.shuffleNode()) {
//            node = new ShuffleProcessorNode<>(name, parent.getName(), supplier);
//        } else {
//            node = new ProcessorNode<>(name, parent.getName(), supplier);
//        }

        return this.pipeline.addWindowStreamVirtualNode(node, parent, windowInfo);
    }

    @Override
    public RStream<V> toRStream() {
        return new RStreamImpl<>(this.pipeline, parent);
    }

    private String makeName(String prefix) {
        String name;

        /**
         * 左右流join时，需要保证左右流shuffle到相同的topic中，这样，相同key才能到一个计算实例上，所以需要这个name一致
         * 移除是因为避免影响后续算子；
         */
        CommonNameMaker commonName = (CommonNameMaker) this.properties.remove(Constant.COMMON_NAME_MAKER);
        if (commonName != null) {
            name = OperatorNameMaker.makeCommonName(commonName.getPrefix(), commonName.getLocalIndex());
        } else {
            name = OperatorNameMaker.makeName(prefix);
        }

        return name;
    }

    @Override
    public void setProperties(Properties properties) {
        this.properties.putAll(properties);
    }
}
