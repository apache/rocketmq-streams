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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.script.service.IScriptExpression;

public class FilterTreeNode extends TreeNode<FilterChainStage> {
    protected Rule rule;
    protected Set<String> dependentFieldNames;
    protected AbstractStage sourceStage;
    protected AbstractStage nextStage;

    public FilterTreeNode(ChainPipeline pipeline, FilterChainStage stage,
        TreeNode parent) {
        super(pipeline, stage, parent);
        rule = (Rule) stage.getRule();
        dependentFieldNames = rule.getDependentFields();
    }

    public PreFingerprint createPreFingerprint() {
        String filterStageIdentification = MapKeyUtil.createKey(rule.getNameSpace(), this.pipeline.getName(), rule.getName());
        List<String> parents = this.getStage().getPrevStageLabels();

        if (parents == null || this.getParents() == null || this.getParents().size() == 0) {
            boolean containsDimField = containsDimField(dependentFieldNames);
            if (containsDimField) {
                return null;
            }
            PreFingerprint preFingerprint = new PreFingerprint(createFingerprint(dependentFieldNames), filterStageIdentification, pipeline.getChannelName(), this.stage.getLabel(), getExpressionCount(), stage);
            return preFingerprint;
        }
        if (parents.size() > 1) {
            throw new RuntimeException("can not support mutil parent for filter stage");
        }
        TreeNode parent = this.getParents().get(0);
        Set<String> denpendentFields = traceDepenentToSource(parent, this, this.dependentFieldNames);
        boolean containsDimField = containsDimField(denpendentFields);
        if (containsDimField) {
            return null;
        }
        if (denpendentFields == null) {
            return null;
        }
        PreFingerprint preFingerprint = new PreFingerprint(createFingerprint(denpendentFields), filterStageIdentification, sourceStage == null ? null : sourceStage.getLabel(), nextStage.getLabel(), getExpressionCount(), stage);
        return preFingerprint;
    }

    private boolean containsDimField(Set<String> fields) {
        if (fields == null) {
            return false;
        }
        for (String field : fields) {
            if (field.indexOf(".") != -1) {
                return true;
            }
        }
        return false;
    }

    public Set<String> traceDepenentToSource(TreeNode parent, TreeNode current, Set<String> denpendentFields) {
        if (parent != null) {
            sourceStage = parent.getStage();
        } else {
            sourceStage = null;
        }
        nextStage = current.getStage();
        Set<String> newDependents = new HashSet<>();
        boolean isStageBreak = false;
        for (String varName : denpendentFields) {
            AtomicBoolean isBreak = new AtomicBoolean(false);
            List<IScriptExpression> scriptExpressions = new ArrayList<>();
            Set<String> set = parent.traceaField(varName, isBreak, scriptExpressions);
            if (set != null) {
                newDependents.addAll(set);
            }
            if (isBreak.get()) {
                isStageBreak = true;
            }
        }
        if (isStageBreak) {
            return null;
        }
        if (parent.getStage().getPrevStageLabels().size() > 1) {
            return newDependents;
        }
        List<TreeNode> treeNodes = parent.getParents();
        List<String> parents = parent.getStage().getPrevStageLabels();
        if (CollectionUtil.isEmpty(parents) || CollectionUtil.isEmpty(treeNodes)) {
            sourceStage = null;
            nextStage = parent.getStage();
            return newDependents;
        }
        if (parents.size() > 1) {
            throw new RuntimeException("can not optimizate mutil rule stages " + stage.getLabel());
        }
        if (parent.getStage().getNextStageLabels().size() > 1) {
            return newDependents;
        }
        this.nextStage = parent.getStage();

        return traceDepenentToSource(treeNodes.get(0), parent, newDependents);
    }

    @Override
    public List<CommonExpression> traceDepenentToSource() {
        return traceDepenentToSource(rule);
    }

    @Override
    public Set<String> traceaField(String varName, AtomicBoolean isBreak, List<IScriptExpression> scriptExpressions) {
        Set<String> result = new HashSet<>();
        result.add(varName);
        return result;
    }

    protected String createFingerprint(Set<String> fieldNames) {
        List<String> fieldNameList = new ArrayList<>(fieldNames);
        Collections.sort(fieldNameList);
        return MapKeyUtil.createKey(",", fieldNameList);
    }

    public int getExpressionCount() {
        Collection<Expression> expressionSet = this.rule.getExpressionMap().values();
        int count = 0;
        for (Expression expression : expressionSet) {
            if (!RelationExpression.class.isInstance(expression)) {
                continue;
            }
            count++;
        }
        return count;
    }

}
