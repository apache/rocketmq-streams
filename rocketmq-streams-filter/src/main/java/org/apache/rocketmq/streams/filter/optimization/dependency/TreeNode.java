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
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.operator.expression.GroupScriptExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public abstract class TreeNode<T extends AbstractStage> {
    protected List<TreeNode> parents = new ArrayList<>();
    protected List<TreeNode> children = new ArrayList<>();
    protected String name;
    protected ChainPipeline pipeline;
    protected T stage;
    protected MetaData soureMetaData;

    public TreeNode(ChainPipeline pipeline, T stage, TreeNode parent) {
        this.pipeline = pipeline;
        this.stage = stage;
        if (parent != null) {
            parents.add(parent);
            parent.getChildren().add(this);
        }
        soureMetaData = pipeline.getChannelMetaData();
    }

    public abstract Set<String> traceaField(String varName, AtomicBoolean isBreak, List<IScriptExpression> depenentScript);

    /**
     * If the ETL procedure of the expression dependent variable can be traced to the source, the dependent expression is returned; otherwise, null is returned
     *
     * @return
     */
    public abstract List<CommonExpression> traceDepenentToSource();

    public boolean isSourceField(String varNames) {
        return this.soureMetaData.getMetaDataField(varNames) != null;
    }

    public boolean traceDepenentToSource(TreeNode parent, CommonExpression commonExpression, String varName) {
        AtomicBoolean isBreak = new AtomicBoolean(false);
        List<IScriptExpression> scriptExpressions = new ArrayList<>();
        Set<String> varNames = parent.traceaField(varName, isBreak, scriptExpressions);
        if (containsGroupScript(scriptExpressions)) {
            return false;
        }
        if (isBreak.get()) {
            return false;
        }
        if (parent.getStage().getPrevStageLabels().size() > 1) {
            return false;
        }
        List<TreeNode> treeNodes = parent.getParents();
        List<String> parents = parent.getStage().getPrevStageLabels();
        if (CollectionUtil.isEmpty(parents) || CollectionUtil.isEmpty(treeNodes)) {
            commonExpression.addPreviewScriptDependent(scriptExpressions);
            return true;
        }
        if (parents.size() > 1) {
            return false;
        }
        commonExpression.addPreviewScriptDependent(scriptExpressions);

        for (String newDependentVarName : varNames) {
            boolean success = traceDepenentToSource(treeNodes.get(0), commonExpression, newDependentVarName);
            if (!success) {
                return false;
            }
        }
        return true;
    }

    protected List<CommonExpression> traceDepenentToSource(Rule rule) {
        Collection<Expression> expressions = rule.getExpressionMap().values();
        List<String> parents = this.getStage().getPrevStageLabels();
        List<CommonExpression> commonExpressions = new ArrayList<>();
        for (Expression expression : expressions) {
            List<CommonExpression> commonExpressionList = traceDepenentToSource(expression);
            if (commonExpressionList != null) {
                commonExpressions.addAll(commonExpressionList);
            }
        }
        return commonExpressions;
    }

    protected List<CommonExpression> traceDepenentToSource(Expression expression) {
        List<String> parents = this.getStage().getPrevStageLabels();
        List<CommonExpression> commonExpressions = new ArrayList<>();
        if (RelationExpression.class.isInstance(expression)) {
            return null;
        }
        String functionName = expression.getFunctionName();
        if (functionName == null) {
            return null;
        }
        if (CommonExpression.support(expression)) {
            if (parents == null || this.getParents() == null || this.getParents().size() == 0) {
                CommonExpression commonExpression = new CommonExpression(expression);
                commonExpressions.add(commonExpression);
                return commonExpressions;
            }
            if (parents.size() > 1) {
                throw new RuntimeException("can not support mutil parent for filter stage");
            }
            TreeNode parent = this.getParents().get(0);
            CommonExpression commonExpression = traceDepenentToSource(parent, expression);
            if (commonExpression != null) {
                commonExpressions.add(commonExpression);
            }
        }
        return commonExpressions;
    }

    protected CommonExpression traceDepenentToSource(TreeNode parent, Expression expression) {
        if (!CommonExpression.support(expression)) {
            return null;
        }
        CommonExpression commonExpression = new CommonExpression(expression);
        if (parent == null) {
            return commonExpression;
        }
        traceDepenentToSource(parent, commonExpression, expression.getVarName());
        return commonExpression;
    }

    protected boolean containsGroupScript(List<IScriptExpression> expressions) {
        for (IScriptExpression scriptExpression : expressions) {
            if (GroupScriptExpression.class.isInstance(scriptExpression)) {
                return true;
            }
        }
        return false;
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
