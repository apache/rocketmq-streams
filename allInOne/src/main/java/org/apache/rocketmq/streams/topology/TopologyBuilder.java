package org.apache.rocketmq.streams.topology;
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

import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.state.StateStore;
import org.apache.rocketmq.streams.topology.real.ProcessorFactory;
import org.apache.rocketmq.streams.topology.real.RealProcessorFactory;
import org.apache.rocketmq.streams.topology.real.SinkFactory;
import org.apache.rocketmq.streams.topology.real.SourceFactory;
import org.apache.rocketmq.streams.topology.real.StatefulProcessorFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class TopologyBuilder {
    private final LinkedHashMap<String/*source topic*/, SourceFactory<?, ?, ?, ?>> topic2SourceNodeFactory = new LinkedHashMap<>();

    private final LinkedHashMap<String/*name*/, RealProcessorFactory<?, ?, ?, ?>> realNodeFactory = new LinkedHashMap<>();

    private final LinkedHashMap<String/*sink topic*/, RealProcessorFactory<?, ?, ?, ?>> topic2SinkNodeFactory = new LinkedHashMap<>();

    private final HashMap<String/*source name*/, List<String/*subsequent processor without source*/>> source2Group = new HashMap<>();

    private final HashMap<String, StatefulProcessorFactory<?, ?, ?, ?>> name2StateStore = new HashMap<>();

    public <K, V, OK, OV> void addRealSource(String name, String topicName, Supplier<? extends Processor<K, V, OK, OV>> supplier) {
        SourceFactory<K, V, OK, OV> sourceFactory = new SourceFactory<>(name, topicName, supplier);

        realNodeFactory.put(name, sourceFactory);

        topic2SourceNodeFactory.put(topicName, sourceFactory);

        //将source与sink之间的节点分为一个组，处理数据时，不同分组使用不同task
        source2Group.put(name, new ArrayList<>());
    }

    @SuppressWarnings("unchecked")
    public <K, V, OK, OV> void addRealNode(String name, String parentName, Supplier<? extends Processor<K, V, OK, OV>> supplier) {
        RealProcessorFactory<K, V, OK, OV> processorFactory = new ProcessorFactory<>(name, supplier);
        realNodeFactory.put(name, processorFactory);

        RealProcessorFactory<K, V, OK, OV> parentFactory = (RealProcessorFactory<K, V, OK, OV>) realNodeFactory.get(parentName);
        parentFactory.addChild(processorFactory);

        grouping(name, parentName);
    }


    @SuppressWarnings("unchecked")
    public <K, V, OK, OV> void addStatefulRealNode(String name, String parentName, StateStore<K, V> stateStore, Supplier<? extends Processor<K, V, OK, OV>> supplier) {
        StatefulProcessorFactory<K, V, OK, OV> processorFactory = new StatefulProcessorFactory<>(name, supplier);
        processorFactory.setStateStore(stateStore);
        realNodeFactory.put(name, processorFactory);

        name2StateStore.put(name, processorFactory);

        RealProcessorFactory<K, V, OK, OV> parentFactory = (RealProcessorFactory<K, V, OK, OV>) realNodeFactory.get(parentName);
        parentFactory.addChild(processorFactory);

        grouping(name, parentName);
    }

    @SuppressWarnings("unchecked")
    public <K, V, OK, OV> void addRealSink(String name, String parentName, String topicName, Supplier<? extends Processor<K, V, OK, OV>> supplier) {
        SinkFactory<K, V, OK, OV> sinkFactory = new SinkFactory<>(name, supplier);
        realNodeFactory.put(name, sinkFactory);
        topic2SinkNodeFactory.put(topicName, sinkFactory);

        RealProcessorFactory<K, V, OK, OV> parentFactory = (RealProcessorFactory<K, V, OK, OV>) realNodeFactory.get(parentName);
        parentFactory.addChild(sinkFactory);

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

//    @SuppressWarnings("unchecked")
//    public <K, V> StatefulProcessorFactory<K, V> getStatefulProcessorFactory(String name) {
//        return (StatefulProcessorFactory<K, V>) this.name2StateStore.get(name);
//    }

    public Set<String> getSourceTopic() {
        return Collections.unmodifiableSet(this.topic2SourceNodeFactory.keySet());
    }

//    @SuppressWarnings("unchecked")
//    public <K, V, OK, OV> List<RealProcessorFactory<K, V, OK, OV>> getProcessorFactoryGroup(String topicName) {
//        SourceFactory<K, V, OK, OV> sourceFactory = (SourceFactory<K, V, OK, OV>) topic2SourceNodeFactory.get(topicName);
//
//        String sourceName = sourceFactory.getName();
//        List<String> groupNames = source2Group.get(sourceName);
//
//        List<RealProcessorFactory<K, V, OK, OV>> result = new ArrayList<>();
//        result.add(sourceFactory);
//
//        for (String name : groupNames) {
//            RealProcessorFactory<K, V, OK, OV> processorFactory = (RealProcessorFactory<K, V, OK, OV>) realNodeFactory.get(name);
//            result.add(processorFactory);
//        }
//
//        return result;
//    }


//    @SuppressWarnings("unchecked")
//    public <K, V, OK, OV> Task buildTask(String topicName) {
//        SourceFactory<K, V, OK, OV> sourceFactory = (SourceFactory<K, V, OK, OV>) topic2SourceNodeFactory.get(topicName);
//        Processor<K, V, OK, OV> sourceProcessor = sourceFactory.build();
//
//        List<RealProcessorFactory<K, V, OK, OV>> children = sourceFactory.getChildren();
//        for (RealProcessorFactory<K, V, OK, OV> child : children) {
//            Processor<K, V, OK, OV> build = child.build();
//        }
//
//
//    }


    @SuppressWarnings("unchecked")
    public <K, V, OK, OV> Processor<K, V, OK, OV> build(String topicName) {
        SourceFactory<K, V, OK, OV> sourceFactory = (SourceFactory<K, V, OK, OV>) topic2SourceNodeFactory.get(topicName);
        Processor<K, V, OK, OV> sourceProcessor = sourceFactory.build();

        String sourceName = sourceFactory.getName();
        //集合中的顺序就是算子的父子顺序，前面的是后面的父亲节点
        List<String> groupNames = source2Group.get(sourceName);

        doBuild(sourceProcessor, sourceFactory.getChildren());

        return sourceProcessor;
    }

    private <K, V, OK, OV> void doBuild(final Processor<K, V, OK, OV> parent, List<RealProcessorFactory<K, V, OK, OV>> childrenFactory) {

        for (RealProcessorFactory<K, V, OK, OV> childRealProcessorFactory : childrenFactory) {
            Processor<K, V, OK, OV> child = childRealProcessorFactory.build();
            parent.addChild(child);

            doBuild(child, childRealProcessorFactory.getChildren());
        }
    }



}
