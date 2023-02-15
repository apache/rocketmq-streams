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

import org.apache.rocketmq.streams.core.function.FilterAction;
import org.apache.rocketmq.streams.core.function.ForeachAction;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.serialization.KeyValueSerializer;

public interface RStream<T> {
    RStream<T> selectTimestamp(ValueMapperAction<T, Long> timestampSelector);

    <O> RStream<O> map(ValueMapperAction<T, O> mapperAction);

    <VR> RStream<VR> flatMap(final ValueMapperAction<T, ? extends Iterable<? extends VR>> mapper);

    RStream<T> filter(FilterAction<T> predictor);

    <K> GroupedStream<K, T> keyBy(SelectAction<K, T> selectAction);

    void print();

    RStream<T> foreach(ForeachAction<T> foreachAction);

    <T2> JoinedStream<T, T2> join(RStream<T2> rightStream);

    <T2> JoinedStream<T, T2> leftJoin(RStream<T2> rightStream);

    Pipeline getPipeline();

    void sink(String topicName, KeyValueSerializer<Object, T> serializer);
}
