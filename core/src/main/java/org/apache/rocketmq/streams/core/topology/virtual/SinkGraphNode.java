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

import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;

import java.util.function.Supplier;

public class SinkGraphNode<T> extends AbstractGraphNode {
    private final Supplier<Processor<T>> supplier;
    private final String topicName;
    private final String parentName;

    public SinkGraphNode(String name, String parentName, String topicName, Supplier<Processor<T>> supplier) {
        super(name);
        this.topicName = topicName;
        this.supplier = supplier;
        this.parentName = parentName;
    }


    @Override
    public void addRealNode(TopologyBuilder builder) {
        builder.addRealSink(name, parentName, topicName, supplier);
    }
}
