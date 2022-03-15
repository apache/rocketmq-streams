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
package org.apache.rocketmq.streams.common.optimization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;

public class TaskOptimization {
    protected int homologousExpressionCaseSize = 1000000;
    protected int preFingerprintCaseSize = 1000000;

    /**
     * Automatically parses pipelines, generates pre-filter fingerprints and expression estimates
     */
    protected transient volatile IHomologousOptimization homologousOptimization;

    public TaskOptimization(int homologousExpressionCaseSize, int preFingerprintCaseSize) {
        this.homologousExpressionCaseSize = homologousExpressionCaseSize;
        this.preFingerprintCaseSize = preFingerprintCaseSize;
    }

    public void openOptimization(ChainPipeline<?>... pipelines) {
        if (pipelines == null) {
            return;
        }
        List<ChainPipeline<?>> pipelineList = new ArrayList<>();
        Collections.addAll(pipelineList, pipelines);
        openOptimization(pipelineList);
    }

    public void openOptimization(List<ChainPipeline<?>> pipelines) {
        if (this.homologousOptimization == null) {
            synchronized (this) {
                if (this.homologousOptimization == null) {
                    Iterable<IHomologousOptimization> iterable = ServiceLoader.load(IHomologousOptimization.class);
                    Iterator<IHomologousOptimization> it = iterable.iterator();
                    if (it.hasNext()) {
                        this.homologousOptimization = it.next();
                        this.homologousOptimization.optimizate(pipelines, this.homologousExpressionCaseSize, this.preFingerprintCaseSize);
                    }
                }
            }
        }
    }

    public void calculateOptimizationExpression(IMessage message, AbstractContext context) {
        if (homologousOptimization != null) {
            homologousOptimization.calculate(message, context);
        }
    }
}
