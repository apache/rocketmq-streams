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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * raverse the pipeline to create a prefix filter fingerprint
 */
public class DependencyTree {

    protected ChainPipeline chainPipeline;
    protected List<CommonExpression> commonExpressions;
    protected transient Map<String, Map<String, PreFingerprint>> preFingerprintExecutor = new HashMap<>();
//    protected FingerprintCache fingerprintCache;

    public DependencyTree(ChainPipeline pipeline) {
        this.chainPipeline = pipeline;
    }

    /**
     * parse pipeline 2 DependencyTree
     */
    public List<CommonExpression> parse() {
        List<CommonExpression> commonExpressions = null;
        if (chainPipeline.isTopology()) {
            commonExpressions = parseTopology(chainPipeline);
        } else {
            return null;
        }
        for (Map<String, PreFingerprint> fingerprintMap : preFingerprintExecutor.values()) {
            for (PreFingerprint fingerprint : fingerprintMap.values()) {
                fingerprint.getFilterChainStage().setPreFingerprint(fingerprint);
                for (AbstractStage<?> previewFilterChainStage : fingerprint.getAllPreviewFilterChainStage()) {
                    previewFilterChainStage.setPreFingerprint(fingerprint);
                }
            }
        }

        List<CommonExpression> initSuccessCommonExpressions = new ArrayList<>();
        for (CommonExpression commonExpression : commonExpressions) {
            boolean success = commonExpression.init();
            if (success) {
                initSuccessCommonExpressions.add(commonExpression);
            }
        }
        this.commonExpressions = initSuccessCommonExpressions;
        // System.out.println("finish homologous optimization");
        return initSuccessCommonExpressions;
    }

    /**
     * Parse topology pipeline
     *
     * @param pipeline
     */
    public List<CommonExpression> parseTopology(ChainPipeline pipeline) {
        if (StateLessDependencyTree.cache.containsKey(pipeline)) {
            return StateLessDependencyTree.cache.get(pipeline);
        }
        List<String> nextLalbes = pipeline.getChannelNextStageLabel();
        List<CommonExpression> commonExpressions = new ArrayList<>();
        parseTree(null, nextLalbes, pipeline, commonExpressions);
        StateLessDependencyTree.cache.put(chainPipeline, commonExpressions);
        return commonExpressions;
    }

    /**
     * @param parentTreeNode
     * @param nextLables
     * @param pipeline
     */
    protected void parseTree(TreeNode parentTreeNode, List<String> nextLables, ChainPipeline pipeline,
        List<CommonExpression> commonExpressions) {
        if (CollectionUtil.isEmpty(nextLables)) {
            return;
        }
        for (String lable : nextLables) {
            AbstractStage stage = (AbstractStage) pipeline.getStageMap().get(lable);
            TreeNode treeNode;
            if (stage.isAsyncNode()) {
                continue;
            }
            if (ScriptChainStage.class.isInstance(stage)) {
                treeNode = new ScriptTreeNode(pipeline, (ScriptChainStage) stage, parentTreeNode);
                List<CommonExpression> commonExpressionList = treeNode.traceDepenentToSource();
                if (commonExpressionList != null) {
                    commonExpressions.addAll(commonExpressionList);
                }
            } else if (FilterChainStage.class.isInstance(stage)) {
                FilterTreeNode filterTreeNode = new FilterTreeNode(pipeline, (FilterChainStage) stage, parentTreeNode);
                PreFingerprint preFingerprint = filterTreeNode.createPreFingerprint();
                if (preFingerprint == null) {
                    continue;
                }
                boolean isContinue = mergePreFingerprint(preFingerprint, pipeline);
                if (!isContinue) {
                    continue;
                }
                List<CommonExpression> commonExpressionList = filterTreeNode.traceDepenentToSource();
                if (commonExpressionList != null) {
                    commonExpressions.addAll(commonExpressionList);
                }
                treeNode = filterTreeNode;
            } else {
                continue;
            }
            parseTree(treeNode, stage.getNextStageLabels(), pipeline, commonExpressions);
        }

    }

    /**
     * If the two pre filters are one branch, merge and replace the previous one with the latter one
     * The consolidation condition is that the following branches have no new fingerprint fields or more filtering conditions
     *
     * @param fingerprint
     * @param pipeline
     * @return
     */
    protected boolean mergePreFingerprint(PreFingerprint fingerprint, ChainPipeline pipeline) {
        String sourceLable = fingerprint.getSourceStageLabel();
        if (sourceLable == null) {
            sourceLable = pipeline.getChannelName();
        }
        Map<String, Map<String, PreFingerprint>> preFingerprintExecutor = this.preFingerprintExecutor;
        Map<String, PreFingerprint> preFingerprintMap = preFingerprintExecutor.get(sourceLable);
        if (preFingerprintMap == null) {
            preFingerprintMap = new HashMap<>();
            preFingerprintMap.put(fingerprint.getNextStageLabel(), fingerprint);
            preFingerprintExecutor.put(sourceLable, preFingerprintMap);
            return true;
        }
        PreFingerprint previewPreFingerprint = preFingerprintMap.get(fingerprint.getNextStageLabel());
        if (previewPreFingerprint != null && !mergeFingerprint(previewPreFingerprint, fingerprint)) {
            return false;
        }
        if (previewPreFingerprint != null) {
            fingerprint.addPreviwFilterChainStage(previewPreFingerprint.getAllPreviewFilterChainStage());
            fingerprint.addPreviwFilterChainStage(previewPreFingerprint.getFilterChainStage());
        }
        preFingerprintMap.put(fingerprint.getNextStageLabel(), fingerprint);
        return true;
    }

    /**
     * If the two pre filters are one branch, merge and replace the previous one with the latter one
     * The consolidation condition is that the following branches have no new fingerprint fields or more filtering conditions
     *
     * @return
     */
    protected boolean mergeFingerprint(PreFingerprint preview, PreFingerprint current) {
        Set<String> previewLogFingerFieldNameSet = loadLogFingerFieldNames(preview);
        Set<String> currentLogFingerFieldNameSet = loadLogFingerFieldNames(current);
        boolean inPrew = true;
        if (CollectionUtil.isEmpty(currentLogFingerFieldNameSet)) {
            return false;
        }
        for (String name : currentLogFingerFieldNameSet) {
            if (!previewLogFingerFieldNameSet.contains(name)) {
                inPrew = false;
                break;
            }
        }
        if (inPrew) {
            currentLogFingerFieldNameSet.addAll(previewLogFingerFieldNameSet);
            current.setLogFingerFieldNames(currentLogFingerFieldNameSet);
            return true;
        }
        if (current.getExpressionCount() > 10 && current.getExpressionCount() - preview.getExpressionCount() > 5) {
            currentLogFingerFieldNameSet.addAll(previewLogFingerFieldNameSet);
            current.setLogFingerFieldNames(currentLogFingerFieldNameSet);
            return true;
        }
        return false;
    }

    /**
     * Load the fingerprint data of the branch from the pipeline
     *
     * @param preFingerprint
     * @return
     */
    protected Set<String> loadLogFingerFieldNames(PreFingerprint preFingerprint) {
        if (preFingerprint.getLogFingerFieldNames() == null) {
            return new HashSet<>();
        }
        String[] logFingerFieldNames = preFingerprint.getLogFingerFieldNames().split(",");
        Set<String> logFingerFieldNameSet = new HashSet<>();
        for (String logFingerName : logFingerFieldNames) {
            logFingerFieldNameSet.add(logFingerName);
        }
        return logFingerFieldNameSet;
    }

    public Map<String, Map<String, PreFingerprint>> getPreFingerprintExecutor() {
        return preFingerprintExecutor;
    }

    public void setPreFingerprintExecutor(
        Map<String, Map<String, PreFingerprint>> preFingerprintExecutor) {
        this.preFingerprintExecutor = preFingerprintExecutor;
    }

    protected List<CommonExpression> parseSimple(ChainPipeline pipeline) {
        throw new RuntimeException("can not support this method");
    }

    public List<CommonExpression> getCommonExpressions() {
        return commonExpressions;
    }
}
