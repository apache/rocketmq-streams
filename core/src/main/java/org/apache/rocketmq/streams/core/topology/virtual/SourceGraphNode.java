package org.apache.rocketmq.streams.core.topology.virtual;
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

import org.apache.rocketmq.streams.core.function.supplier.SourceSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;

import java.util.function.Supplier;

public class SourceGraphNode<T> extends AbstractGraphNode {
    private Supplier<Processor<T>> supplier;
    private String topicName;


    public SourceGraphNode(String name, String topicName, KeyValueDeserializer<Void, T> deserializer) {
        super(name);
        this.topicName = topicName;
        this.supplier = new SourceSupplier<>(topicName, deserializer);
    }

    @Override
    public void addRealNode(TopologyBuilder builder) {
        builder.addRealSource(name, topicName, supplier);
    }


}
