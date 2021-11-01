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
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;

public abstract class TreeNode<T extends AbstractStage> {
    protected List<TreeNode> parents=new ArrayList<>();
    protected List<TreeNode> children=new ArrayList<>();
    protected String name;
    protected ChainPipeline pipeline;
    protected T stage;
    protected MetaData soureMetaData;
    public TreeNode(ChainPipeline pipeline,T stage,TreeNode parent){
        this.pipeline=pipeline;
        this.stage=stage;
        if(parent!=null){
            parents.add(parent);
            parent.getChildren().add(this);
        }
        soureMetaData=pipeline.getChannelMetaData();
    }

    public abstract Set<String> traceaField(String varName) ;

    public boolean isSourceField(String varNames){
        return this.soureMetaData.getMetaDataField(varNames)!=null;
    }

    public List<TreeNode> getParents() {
        return parents;
    }

    public void setParents(List<TreeNode> parents) {
        this.parents = parents;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void setChildren(List<TreeNode> children) {
        this.children = children;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public T getStage() {
        return stage;
    }
}
