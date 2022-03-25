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


import org.apache.rocketmq.streams.function.ForeachAction;
import org.apache.rocketmq.streams.function.FilterAction;
import org.apache.rocketmq.streams.function.MapperAction;
import org.apache.rocketmq.streams.function.ValueMapperAction;

public interface RStream<K, V> {

    <OV> RStream<K, OV> map(ValueMapperAction<V, OV> mapperAction);

    RStream<K, V> filter(FilterAction<K, V> predictor);

    <R> GroupedStream<R,V> groupBy(MapperAction<K, V, R> mapperAction);

    void print();

    void foreach(ForeachAction<K, V> foreachAction);

    void sink(String topicName);
}
