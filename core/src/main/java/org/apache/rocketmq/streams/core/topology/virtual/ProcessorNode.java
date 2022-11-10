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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ProcessorNode<T> extends AbstractGraphNode {
    protected final Supplier<Processor<T>> supplier;
    protected final List<String> parentNames;
    protected boolean shuffle = false;


    public ProcessorNode(String name, String parentName, Supplier<Processor<T>> supplier) {
        super(name);
        this.supplier = supplier;
        this.parentNames = new ArrayList<>();
        this.parentNames.add(parentName);
    }

    public ProcessorNode(String name, List<String> parentNames, Supplier<Processor<T>> supplier) {
        super(name);
        this.supplier = supplier;
        this.parentNames = parentNames;
    }

    public ProcessorNode(String name, List<String> parentNames, boolean shuffle, Supplier<Processor<T>> supplier) {
        super(name);
        this.supplier = supplier;
        this.parentNames = parentNames;
        this.shuffle = shuffle;
    }

    public ProcessorNode(String name, String parentName, boolean shuffle, Supplier<Processor<T>> supplier) {
        super(name);
        this.supplier = supplier;
        this.parentNames = new ArrayList<>();
        this.parentNames.add(parentName);
        this.shuffle = shuffle;
    }

    @Override
    public boolean shuffleNode() {
        return this.shuffle;
    }


    @Override
    public void addRealNode(TopologyBuilder builder) {
        for (String parentName : parentNames) {
            builder.addRealNode(name, parentName, supplier);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessorNode<?> that = (ProcessorNode<?>) o;
        return this.name.equals(that.name);
    }

    @Override
    public String toString() {
        return "ProcessorNode{" + "name=[" + name + "]}";
    }
}
