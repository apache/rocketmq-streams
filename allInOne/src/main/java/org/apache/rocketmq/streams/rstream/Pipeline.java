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

import org.apache.rocketmq.streams.topology.TopologyBuilder;
import org.apache.rocketmq.streams.topology.real.RealProcessorFactory;
import org.apache.rocketmq.streams.topology.virtual.AbstractGraphNode;
import org.apache.rocketmq.streams.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.topology.virtual.SinkGraphNode;
import org.apache.rocketmq.streams.topology.virtual.SourceGraphNode;

import java.util.ArrayList;
import java.util.List;

public class Pipeline {
    private final List<GraphNode> virtualNodes = new ArrayList<>();
    private final GraphNode root = new AbstractGraphNode("root") {
        @Override
        public void addRealNode(TopologyBuilder builder) {
            //no-op
        }
    };

    public <K, V> RStream<K, V> addVirtualSource(GraphNode sourceGraphNode) {
        root.addChild(sourceGraphNode);
        virtualNodes.add(sourceGraphNode);

        return new RStreamImpl<>(this, sourceGraphNode);
    }

    public <K, V> RStream<K, V> addVirtualNode(GraphNode currentNode, GraphNode parentNode) {
        parentNode.addChild(currentNode);
        currentNode.addParent(parentNode);

        virtualNodes.add(currentNode);
        return new RStreamImpl<>(this, currentNode);
    }

    public <K, V> GroupedStreamImpl<K, V> addVirtual(GraphNode currentNode, GraphNode parentNode) {
        parentNode.addChild(currentNode);
        currentNode.addParent(parentNode);

        virtualNodes.add(currentNode);
        return new GroupedStreamImpl<>(this, currentNode, currentNode.shuffleNode());
    }

    public void addVirtualSink(GraphNode currentNode, GraphNode parentNode) {
        parentNode.addChild(currentNode);
        virtualNodes.add(currentNode);
    }

    public GraphNode getRoot() {
        return this.root;
    }
}
