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
import java.util.Map;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;

public class PipelineTree {

    protected ChainPipeline pipeline;
    protected Map<String, AbstractStage> stageMap;

    public PipelineTree(ChainPipeline pipeline) {
        this.pipeline = pipeline;
        stageMap = pipeline.getStageMap();
    }

    protected class Context {

    }

    public void registePreFingerprint(Context context, TreeNode parent, List<String> nextLabels) {
        TreeNode current = null;
        if (nextLabels != null) {
            for (String nextLable : nextLabels) {
                AbstractStage stage = stageMap.get(nextLable);
                if (stage instanceof ScriptChainStage) {
                    current = new ScriptTreeNode(pipeline, (ScriptChainStage) stage, parent);
                } else if (stage instanceof FilterChainStage) {
                    current = new FilterTreeNode(pipeline, (FilterChainStage) stage, parent);
                } else {
                    continue;
                }
                registePreFingerprint(context, current, current.getStage().getNextStageLabels());
            }
        }
    }

}
