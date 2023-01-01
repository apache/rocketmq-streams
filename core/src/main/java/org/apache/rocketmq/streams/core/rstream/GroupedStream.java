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

import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;

import java.util.function.Supplier;

public interface GroupedStream<K, V> {

    GroupedStream<K, Integer> count();

    <OUT> GroupedStream<K, Integer> count(SelectAction<OUT, V> selectAction);


    GroupedStream<K, V> min(SelectAction<? extends Number, V> selectAction);


    GroupedStream<K, V> max(SelectAction<? extends Number, V> selectAction);


    GroupedStream<K, ? extends Number> sum(SelectAction<? extends Number, V> selectAction);


    GroupedStream<K, V> filter(FilterAction<V> predictor);

    <OUT> GroupedStream<K, OUT> map(ValueMapperAction<V, OUT> valueMapperAction);


    <OUT> GroupedStream<K, OUT> aggregate(Accumulator<V, OUT> accumulator);

    WindowStream<K, V> window(WindowInfo windowInfo);

    GroupedStream<K, V> addGraphNode(String name, Supplier<Processor<V>> supplier);

    RStream<V> toRStream();

    void sink(String topicName, KeyValueSerializer<K, V> serializer);
}
