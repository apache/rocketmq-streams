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
package org.apache.rocketmq.streams.filter.optimization.homologous;

import com.alibaba.fastjson.JSONObject;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.filter.optimization.dependency.CommonExpression;
import org.apache.rocketmq.streams.filter.optimization.dependency.DependencyTree;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(IHomologousOptimization.class)
public class HomologousOptimization implements IHomologousOptimization {

    private static final Logger LOGGER = LoggerFactory.getLogger(HomologousOptimization.class);
    protected transient HomologousCompute homologousCompute;
    protected static Map<ChainPipeline<?>, HomologousCompute> homologousComputeCache = new HashMap<>();

    @Override
    public void optimizate(List<ChainPipeline<?>> pipelines, int cacheSize, int preFingerprintCacheSize) {
        List<CommonExpression> commonExpressions = new ArrayList<>();
        FingerprintCache fingerprintCache = new FingerprintCache(preFingerprintCacheSize);
        for (ChainPipeline<?> pipeline : pipelines) {
            DependencyTree dependencyTree = new DependencyTree(pipeline, fingerprintCache);
            List<CommonExpression> commonExpressionList = dependencyTree.parse();
            if (commonExpressionList != null) {
                commonExpressions.addAll(commonExpressionList);
            }
            printOptimizePipeline(pipeline);
        }
        homologousCompute = new HomologousCompute(commonExpressions, cacheSize);
        for (ChainPipeline<?> chainPipeline : pipelines) {
            homologousComputeCache.put(chainPipeline, homologousCompute);
        }
    }

    public HomologousCompute getHomologousCompute(ChainPipeline<?> chainPipeline) {
        return homologousComputeCache.get(chainPipeline);
    }

    @Override
    public void calculate(IMessage message, AbstractContext context) {
        if (homologousCompute != null) {
            homologousCompute.calculate(message, context);
        }

    }

    protected void printOptimizePipeline(ChainPipeline<?> pipeline) {
        Map<String, Map<String, PreFingerprint>> prefingers = pipeline.getPreFingerprintExecutor();
        JSONObject detail = new JSONObject();
        for (String prefinger : prefingers.keySet()) {
            Map<String, PreFingerprint> branchs = prefingers.get(prefinger);
            for (String branchName : branchs.keySet()) {
                PreFingerprint preFingerprint = branchs.get(branchName);
                detail.put("prefiger." + (prefinger.equals(pipeline.getChannelName()) ? "source" : prefinger), preFingerprint.getLogFingerFieldNames());
            }
        }
        LOGGER.info("[{}][{}] Finish_Optimize_Detail_Is_[{}]", IdUtil.instanceId(), pipeline.getConfigureName(), JsonableUtil.formatJson(detail));
    }

}
