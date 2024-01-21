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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.EmptyChainStage;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class EmptyTreeNode extends TreeNode<EmptyChainStage> {
    public EmptyTreeNode(ChainPipeline pipeline,
        EmptyChainStage stage, TreeNode parent) {
        super(pipeline, stage, parent);
    }

    @Override
    public Set<String> traceaField(String varName, AtomicBoolean isBreak, List<IScriptExpression> scriptExpressions) {
        Set<String> result = new HashSet<>();
        result.add(varName);
        return result;
    }

    @Override public List<CommonExpression> traceDepenentToSource() {
        return new ArrayList<>();
    }
}
