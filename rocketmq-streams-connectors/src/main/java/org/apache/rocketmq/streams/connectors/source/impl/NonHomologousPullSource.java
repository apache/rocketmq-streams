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
package org.apache.rocketmq.streams.connectors.source.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.connectors.reader.ISplitReader;
import org.apache.rocketmq.streams.connectors.source.AbstractPullSource;

/**
 * 多个pull pipeline合并成一个 大的调度体
 */
public class NonHomologousPullSource extends AbstractPullSource {
    protected Map<String, ChainPipeline> pipelines = new HashMap<>();

    @Override protected ISplitReader createSplitReader(ISplit<?, ?> split) {
        SplitProxy splitProxy = (SplitProxy) split;
        AbstractPullSource source = (AbstractPullSource) splitProxy.pipeline.getSource();
        return source.createReader(split);
    }

    @Override public synchronized List<ISplit<?, ?>> fetchAllSplits() {
        List<ISplit<?, ?>> splits = new ArrayList<>();
        for (ChainPipeline pipeline : pipelines.values()) {
            if (!(pipeline.getSource() instanceof AbstractPullSource)) {
                throw new RuntimeException("MultiSplitSource can support AbstractPullSource sql only");
            }
            List<ISplit<?, ?>> pipelineSplits = pipeline.getSource().fetchAllSplits();
            for (ISplit<?, ?> split : pipelineSplits) {
                splits.add(new SplitProxy<>(pipeline, split));
            }

        }
        return splits;
    }

    public synchronized void addSubPipeline(ChainPipeline chainPipeline) {
        if (!(chainPipeline.getSource() instanceof AbstractPullSource)) {
            throw new RuntimeException("MultiSplitSource can support AbstractPullSource sql only");
        }
        this.pipelines.put(chainPipeline.getName(), chainPipeline);
        chainPipeline.startPipeline();

    }

    public synchronized void removeSubPipeline(String chainPipelineName) {
        ChainPipeline chainPipeline = this.pipelines.remove(chainPipelineName);
        if (chainPipeline != null) {
            chainPipeline.destroy();
        }
    }

    @Override public synchronized void destroySource() {
        super.destroySource();
        for (ChainPipeline pipeline : pipelines.values()) {
            pipeline.destroy();
        }

    }

    protected class SplitProxy<T, Q> implements ISplit<T, Q> {
        protected ISplit<T, Q> split;
        protected ChainPipeline pipeline;

        public SplitProxy(ChainPipeline pipeline, ISplit<T, Q> split) {
            this.split = split;
            this.pipeline = pipeline;
        }

        @Override public String getQueueId() {
            return split.getQueueId();
        }

        @Override public Q getQueue() {
            return split.getQueue();
        }

        @Override public int compareTo(T o) {
            return split.compareTo(o);
        }

        @Override public String toJson() {
            return split.toJson();
        }

        @Override public void toObject(String jsonString) {
            split.toObject(jsonString);
        }
    }
}
