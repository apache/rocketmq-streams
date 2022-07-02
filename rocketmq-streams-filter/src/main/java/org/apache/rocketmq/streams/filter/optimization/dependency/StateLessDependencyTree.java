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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * raverse the pipeline to create a prefix filter fingerprint
 */
public class StateLessDependencyTree  extends DependencyTree{
    public static Map<ChainPipeline,List<CommonExpression>> cache=new HashMap<>();


    protected ChainPipeline chainPipeline;
    protected     Map<String, Map<String, PreFingerprint>> preFingerprintExecutor=new HashMap<>();
    public StateLessDependencyTree(ChainPipeline pipeline) {
        super(pipeline,null);
        this.chainPipeline = pipeline;
    }


    /**
     * If the two pre filters are one branch, merge and replace the previous one with the latter one
     * The consolidation condition is that the following branches have no new fingerprint fields or more filtering conditions
     *
     * @param fingerprint
     * @param pipeline
     * @return
     */
    @Override
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


}
