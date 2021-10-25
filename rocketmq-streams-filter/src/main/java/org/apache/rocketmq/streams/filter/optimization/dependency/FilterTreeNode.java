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
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.script.operator.impl.FunctionScript;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class FilterTreeNode extends TreeNode<FilterChainStage> {
   protected Rule rule;
   protected Set<String> dependentFieldNames;
   protected AbstractStage sourceStage;
   protected AbstractStage nextStage;
    public FilterTreeNode(ChainPipeline pipeline, FilterChainStage stage,
        TreeNode parent) {
        super(pipeline, stage, parent);
        if(stage.getRules().size()>1){
            throw new RuntimeException("can not optimizate mutil rule stages "+stage.getLabel());
        }
        rule=(Rule) stage.getRules().get(0);
        dependentFieldNames=rule.getDependentFields();
    }


    public PreFingerprint createPreFingerprint(){
        String filterStageIdentification= MapKeyUtil.createKey(rule.getNameSpace(),this.pipeline.getConfigureName(),rule.getConfigureName());
        List<TreeNode> parents=this.getParents();
        if(parents==null){
            PreFingerprint preFingerprint=new PreFingerprint(createFingerprint(dependentFieldNames),filterStageIdentification,pipeline.getChannelName(),this.stage.getLabel());
            return preFingerprint;
        }
        Set<String> denpendentFields=traceDepenentToSource(parents.get(0),this,this.dependentFieldNames);
        PreFingerprint preFingerprint=new PreFingerprint(createFingerprint(denpendentFields),filterStageIdentification,sourceStage.getLabel(),nextStage.getLabel());
        return preFingerprint;
    }

    public Set<String> traceDepenentToSource(TreeNode parent,TreeNode current,Set<String> denpendentFields){
        if(parent!=null){
            sourceStage=parent.getStage();
        }else {
            sourceStage=null;
        }
        nextStage=current.getStage();

        Set<String> newDependents=new HashSet<>();
        for(String varName:dependentFieldNames){
            Set<String> set= parent.traceaField(varName);
            if(set!=null){
                newDependents.addAll(set);
            }
        }
        if(parent.getChildren().size()>1){
            return newDependents;
        }

        List<TreeNode> parents=parent.getParents();
        if(parents==null){
            sourceStage=null;
            nextStage=current.getStage();
            return newDependents;
        }
        if(parents.size()>1){
            throw new RuntimeException("can not optimizate mutil rule stages "+stage.getLabel());
        }
        return traceDepenentToSource(parents.get(0),parent,newDependents);
    }

    @Override public Set<String> traceaField(String varName) {
        Set<String> result=new HashSet<>();
        result.add(varName);
        return result;
    }

    protected String createFingerprint(Set<String> fieldNames){
        List<String> fieldNameList=new ArrayList<>(fieldNames);
        Collections.sort(fieldNameList);
        return MapKeyUtil.createKey(",",fieldNameList);
    }




}
