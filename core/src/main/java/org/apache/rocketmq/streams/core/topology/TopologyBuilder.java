package org.apache.rocketmq.streams.core.topology;
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
import org.apache.rocketmq.streams.core.topology.real.ProcessorFactory;
import org.apache.rocketmq.streams.core.topology.real.RealProcessorFactory;
import org.apache.rocketmq.streams.core.topology.real.SinkFactory;
import org.apache.rocketmq.streams.core.topology.real.SourceFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class TopologyBuilder {
    private final LinkedHashMap<String/*source topic*/, SourceFactory<?>> topic2SourceNodeFactory = new LinkedHashMap<>();

    private final LinkedHashMap<String/*name*/, RealProcessorFactory<?>> realNodeFactory = new LinkedHashMap<>();

    private final LinkedHashMap<String/*sink topic*/, RealProcessorFactory<?>> topic2SinkNodeFactory = new LinkedHashMap<>();

    private final HashMap<String/*source name*/, List<String/*subsequent processor without source*/>> source2Group = new HashMap<>();


    public <T> void addRealSource(String name, String topicName, Supplier<Processor<T>> supplier) {
        SourceFactory<T> sourceFactory = new SourceFactory<>(name, topicName, supplier);

        realNodeFactory.put(name, sourceFactory);

        topic2SourceNodeFactory.put(topicName, sourceFactory);

        //将source与sink之间的节点分为一个组，处理数据时，不同分组使用不同task
        source2Group.put(name, new ArrayList<>());
    }


    public <T> void addRealNode(String name, String parentName, Supplier<? extends Processor<T>> supplier) {
        RealProcessorFactory<T> processorFactory = new ProcessorFactory<>(name, supplier);
        realNodeFactory.put(name, processorFactory);

        grouping(name, parentName);
    }




    public <T> void addRealSink(String name, String parentName, String topicName, Supplier<Processor<T>> supplier) {
        SinkFactory<T> sinkFactory = new SinkFactory<>(name, supplier);
        realNodeFactory.put(name, sinkFactory);
        topic2SinkNodeFactory.put(topicName, sinkFactory);
        grouping(name, parentName);
    }


    private void grouping(String name, String parentName) {
        if (source2Group.containsKey(parentName)) {
            source2Group.get(parentName).add(name);
        } else {
            for (String sourceName : source2Group.keySet()) {
                List<String> subsequentProcessor = source2Group.get(sourceName);
                if (subsequentProcessor.contains(parentName)) {
                    subsequentProcessor.add(name);
                }
            }
        }
    }


    public Set<String> getSourceTopic() {
        return Collections.unmodifiableSet(this.topic2SourceNodeFactory.keySet());
    }



    @SuppressWarnings("unchecked")
    public <T> Processor<T> build(String topicName) {
        SourceFactory<T> sourceFactory = (SourceFactory<T>) topic2SourceNodeFactory.get(topicName);
        Processor<T> sourceProcessor = sourceFactory.build();

        String sourceName = sourceFactory.getName();

        //集合中的顺序就是算子的父子顺序，前面的是后面的父亲节点
        List<String> groupNames = source2Group.get(sourceName);

        Processor<T> parent = sourceProcessor;
        for (String child : groupNames) {
            RealProcessorFactory<T> childProcessorFactory = (RealProcessorFactory<T>) realNodeFactory.get(child);
            Processor<T> childProcessor = childProcessorFactory.build();
            parent.addChild(childProcessor);
            parent = childProcessor;
        }

        return sourceProcessor;
    }


}
