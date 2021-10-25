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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionChainStage;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class UnionTreeNode extends TreeNode<UnionChainStage> {

    protected  Map<String,List<SimplePipelineTree>> msgSourceName2PipelineList=new HashMap<>();
    public UnionTreeNode(ChainPipeline pipeline, UnionChainStage stage,
        TreeNode parent) {
        super(pipeline, stage, parent);
        initUnion();
    }



    @Override public Set<String> traceaField(String varName) {
       Set<String> varNames=new HashSet<>();
       for(List<SimplePipelineTree> trees:msgSourceName2PipelineList.values()){
           for(SimplePipelineTree simplePipelineTree:trees){
               Set<String> set= simplePipelineTree.traceaField(varName);
               if(set!=null){
                   varNames.addAll(set);
               }
           }
       }
        return varNames;
    }


    protected void initUnion() {
        Map<String, String> piplineName2MsgSourceName= stage.getPiplineName2MsgSourceName();
        for(String pipelineName:piplineName2MsgSourceName.keySet()){
            ChainPipeline pipeline=stage.getPipeline(pipelineName);
            String msgSourceName=piplineName2MsgSourceName.get(pipelineName);
            List<SimplePipelineTree> pipelines=msgSourceName2PipelineList.get(msgSourceName);
            if(pipelines==null){
                pipelines=new ArrayList<>();
                msgSourceName2PipelineList.put(msgSourceName,pipelines);
            }
            pipelines.add(new SimplePipelineTree(pipeline));
        }
    }
}
