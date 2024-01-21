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
import org.apache.rocketmq.streams.common.optimization.IHomologousCalculate;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.metric.NotFireReason;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.expression.Expression;
import org.apache.rocketmq.streams.filter.operator.expression.RelationExpression;
import org.apache.rocketmq.streams.filter.optimization.dependency.CommonExpression;
import org.apache.rocketmq.streams.filter.optimization.dependency.DependencyTree;
import org.apache.rocketmq.streams.script.service.IScriptExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AutoService(IHomologousOptimization.class)
public class HomologousOptimization implements IHomologousOptimization {

    private static final Logger LOGGER = LoggerFactory.getLogger(HomologousOptimization.class);
    protected List<CommonExpression> commonExpressions;
    private Map<ChainPipeline, DependencyTree> dependencyTreeMap = new HashMap<>();

    @Override
    public void optimizate(List<ChainPipeline<?>> pipelines) {
        List<CommonExpression> commonExpressions = new ArrayList<>();
        for (ChainPipeline<?> pipeline : pipelines) {
            DependencyTree dependencyTree = new DependencyTree(pipeline);
            List<CommonExpression> commonExpressionList = dependencyTree.parse();
            if (commonExpressionList != null) {
                commonExpressions.addAll(commonExpressionList);
            }
            dependencyTreeMap.put(pipeline, dependencyTree);
            printOptimizePipeline(pipeline);
        }

        this.commonExpressions = commonExpressions;

    }

    @Override public IHomologousCalculate createHomologousCalculate() {
        HomologousCompute homologousCompute = null;
        homologousCompute = new HomologousCompute(commonExpressions);
        return homologousCompute;
    }

    @Override public void setPeFingerprintForPipeline(ChainPipeline pipeline) {
        /**
         * Create prefix fingerprint objects by branch and Merge branch
         *
         */
        DependencyTree dependencyTree = dependencyTreeMap.get(pipeline);
        pipeline.setPreFingerprintExecutor(dependencyTree.getPreFingerprintExecutor());
    }

    @Override public NotFireReason analysisNotFireReason(FilterChainStage stage, String fieldValue, List<String> notFireExpressionMonitor) {
        NotFireReason notFireReason = new NotFireReason(stage, fieldValue);
        if (notFireReason != null) {
            ChainPipeline<?> chainPipeline = (ChainPipeline) stage.getPipeline();
            DependencyTree dependencyTree = dependencyTreeMap.get(chainPipeline);
            List<CommonExpression> commonExpressions = dependencyTree.getCommonExpressions();
            Map<String, List<String>> filterFieldName2ETLScriptList = new HashMap<>();
            Map<String, String> filterFieldName2OriFieldName = new HashMap<>();
            for (CommonExpression commonExpression : commonExpressions) {
                String filterFieldName = commonExpression.getVarName();
                String origFieldName = commonExpression.getSourceVarName();
                filterFieldName2OriFieldName.put(filterFieldName, origFieldName);
                List<String> etlScript = filterFieldName2ETLScriptList.get(filterFieldName);
                if (etlScript == null) {
                    etlScript = new ArrayList<>();
                    filterFieldName2ETLScriptList.put(filterFieldName, etlScript);
                }
                for (IScriptExpression scriptExpression : commonExpression.getScriptExpressions()) {
                    if (!etlScript.contains(scriptExpression.toString())) {
                        etlScript.add(scriptExpression.toString());
                    }
                }
            }
            List<String> filterFieldNames = new ArrayList<>();
            List<String> expressions = new ArrayList<>();
            List<String> expressionNames = notFireExpressionMonitor;
            Rule rule = (Rule) stage.getRule();
            for (String expressionName : expressionNames) {
                Expression expression = rule.getExpressionMap().get(expressionName);
                if (expression == null) {
                    expressions.add(expressionName);
                } else if (RelationExpression.class.isInstance(expression)) {
                    expressions.add(expression.toExpressionString(rule.getExpressionMap()));
                    filterFieldNames.addAll(expression.getDependentFields(rule.getExpressionMap()));
                } else {
                    filterFieldNames.add(expression.getVarName());
                    expressions.add(expression.toExpressionString(rule.getExpressionMap()));
                }
            }
            notFireReason.setFilterFieldName2ETLScriptList(filterFieldName2ETLScriptList);
            notFireReason.setFilterFieldName2OriFieldName(filterFieldName2OriFieldName);
            notFireReason.setExpressions(expressions);
            notFireReason.setFilterFieldNames(filterFieldNames);
            return notFireReason;
        }
        return null;
    }

    protected void printOptimizePipeline(ChainPipeline<?> pipeline) {
        DependencyTree dependencyTree = dependencyTreeMap.get(pipeline);
        Map<String, Map<String, PreFingerprint>> prefingers = dependencyTree.getPreFingerprintExecutor();
        JSONObject detail = new JSONObject();
        for (String prefinger : prefingers.keySet()) {
            Map<String, PreFingerprint> branchs = prefingers.get(prefinger);
            for (String branchName : branchs.keySet()) {
                PreFingerprint preFingerprint = branchs.get(branchName);
                detail.put("prefiger." + (prefinger.equals(pipeline.getChannelName()) ? "source" : prefinger), preFingerprint.getLogFingerFieldNames());
            }
        }
        LOGGER.info("[{}][{}] Finish_Optimize_Detail_Is_[{}]", IdUtil.instanceId(), pipeline.getName(), JsonableUtil.formatJson(detail));
    }

}
