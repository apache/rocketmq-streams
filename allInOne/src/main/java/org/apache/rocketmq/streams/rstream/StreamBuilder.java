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

import org.apache.rocketmq.streams.OperatorNameMaker;
import org.apache.rocketmq.streams.topology.TopologyBuilder;
import org.apache.rocketmq.streams.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.topology.virtual.SourceGraphNode;

import java.util.List;

import static org.apache.rocketmq.streams.OperatorNameMaker.SOURCE_PREFIX;

public class StreamBuilder {
    private final Pipeline pipeline;
    private final TopologyBuilder topologyBuilder;


    public StreamBuilder() {
        this.pipeline = new Pipeline();
        this.topologyBuilder = new TopologyBuilder();
    }

    public <T> RStream<T> source(String topicName) {
        String name = OperatorNameMaker.makeName(SOURCE_PREFIX);

        GraphNode sourceGraphNode = new SourceGraphNode<>(name, topicName);

        return pipeline.addVirtualSource(sourceGraphNode);
    }

    public TopologyBuilder build() {
        doBuild(pipeline.getRoot());
        return topologyBuilder;
    }

    private void doBuild(GraphNode graphNode) {
        graphNode.addRealNode(topologyBuilder);

        List<GraphNode> allChild = graphNode.getAllChild();
        for (GraphNode node : allChild) {
            doBuild(node);
        }
    }
}
