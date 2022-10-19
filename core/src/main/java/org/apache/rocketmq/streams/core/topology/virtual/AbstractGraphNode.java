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

import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public abstract class AbstractGraphNode implements GraphNode {
    private static final InternalLogger log = ClientLogger.getLog();
    private final List<GraphNode> parents = new ArrayList<>();
    private final List<GraphNode> children = new ArrayList<>();

    protected String name;


    public AbstractGraphNode(String name) {
        Objects.requireNonNull(name, "name can not be null.");
        this.name = name;
    }


    @Override
    public String getName() {
        return name;
    }

    @Override
    public void addParent(GraphNode graphNode) {
        if (!parents.contains(graphNode)) {
            parents.add(graphNode);
        } else {
            log.error("GraphNode: [" + graphNode + "] has exist in parent set.");
        }
    }

    @Override
    public void addChild(GraphNode graphNode) {
        if (!children.contains(graphNode)) {
            children.add(graphNode);
        } else {
            log.error("GraphNode: [" + graphNode + "] has exist in children set.");
        }
    }


    @Override
    public List<GraphNode> getAllChild() {
        return this.children;
    }

    @Override
    public boolean shuffleNode() {
        return false;
    }
}
