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
package org.apache.rocketmq.streams.filter.optimization.dependency;

import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.model.AbstractPipeline;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class SimplePipelineTree {
    protected AbstractPipeline pipeline;
    protected TreeNode rootNode;
    protected TreeNode leafNode;

    public SimplePipelineTree(AbstractPipeline pipeline) {
        this.pipeline = pipeline;
    }

    /**
     * trace root field dependent in this script
     *
     * @param varName
     * @return
     */
    public Set<String> traceaField(String varName) {
        //todo
        return null;
    }

    public List<IScriptExpression> getDependencyExpression(String varName) {
        //todo
        return null;
    }

    public TreeNode getRootNode() {
        return rootNode;
    }

    public void setRootNode(TreeNode rootNode) {
        this.rootNode = rootNode;
    }

    public TreeNode getLeafNode() {
        return leafNode;
    }

    public void setLeafNode(TreeNode leafNode) {
        this.leafNode = leafNode;
    }
}
