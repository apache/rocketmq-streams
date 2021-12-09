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

import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.filter.optimization.dependency.CommonExpression;
import org.apache.rocketmq.streams.filter.optimization.dependency.DependencyTree;

@AutoService(IHomologousOptimization.class)
public class HomologousOptimization implements IHomologousOptimization {
    protected transient HomologousCompute homologousCompute;

    @Override
    public void optimizate(List<ChainPipeline> pipelines, int cacheSize, int preFingerprintCacheSize) {
        List<CommonExpression> commonExpressions = new ArrayList<>();
        FingerprintCache fingerprintCache = new FingerprintCache(preFingerprintCacheSize);
        for (ChainPipeline<?> pipeline : pipelines) {
            DependencyTree dependencyTree = new DependencyTree(pipeline, fingerprintCache);
            List<CommonExpression> commonExpressionList = dependencyTree.parse();
            if (commonExpressionList != null) {
                commonExpressions.addAll(commonExpressionList);
            }
        }
        homologousCompute = new HomologousCompute(commonExpressions, cacheSize);
    }

    @Override
    public void calculate(IMessage message, AbstractContext context) {
        homologousCompute.calculate(message, context);
    }
}
