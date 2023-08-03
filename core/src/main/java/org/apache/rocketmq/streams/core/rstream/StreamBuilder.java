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
package org.apache.rocketmq.streams.core.rstream;

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.topology.TopologyBuilder;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.SourceGraphNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;

import java.util.ArrayList;
import java.util.List;

import static org.apache.rocketmq.streams.core.util.OperatorNameMaker.SOURCE_PREFIX;

public class StreamBuilder {
    private final List<Pipeline> pipelines = new ArrayList<>();
    private final TopologyBuilder topologyBuilder;
    private final String jobId;

    public StreamBuilder(String jobId) throws Throwable {
        checkJobId(jobId);
        this.jobId = jobId;
        this.topologyBuilder = new TopologyBuilder(jobId);
    }

    private void checkJobId(String jobId) {
        if (jobId.startsWith(Constant.WATERMARK_KEY)) {
            throw new IllegalArgumentException("jobId startsWith watermarkPrefix. jodId=" + jobId);
        }
    }

    public <OUT> RStream<OUT> source(String topicName, KeyValueDeserializer<Void, OUT> deserializer) {
        Pipeline pipeline = new Pipeline(jobId);
        this.pipelines.add(pipeline);

        String name = OperatorNameMaker.makeName(SOURCE_PREFIX, jobId);

        GraphNode sourceGraphNode = new SourceGraphNode<>(name, topicName, deserializer);

        return pipeline.addVirtualSource(sourceGraphNode);
    }

    public TopologyBuilder build() {
        //双流join场景中，添加共同节点的pipeline最后构建；三流join未验证。
        pipelines.sort((o1, o2) -> o2.getVirtualNodesNum() - o1.getVirtualNodesNum());

        for (Pipeline pipeline : pipelines) {
            doBuild(pipeline.getRoot());
        }
        return topologyBuilder;
    }

    private void doBuild(GraphNode graphNode) {
        graphNode.addRealNode(topologyBuilder);

        List<GraphNode> allChild = graphNode.getAllChild();
        for (GraphNode node : allChild) {
            doBuild(node);
        }
    }
}
