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

package org.apache.rocketmq.streams.client.transform;

import java.io.Serializable;
import java.util.Set;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.stages.udf.StageBuilder;

public class SplitStream implements Serializable {

    /**
     * 创建datastream时使用
     */
    protected PipelineBuilder pipelineBuilder;
    protected Set<PipelineBuilder> otherPipelineBuilders;
    protected AbstractChainStage<?> currentChainStage;

    public SplitStream(PipelineBuilder pipelineBuilder, Set<PipelineBuilder> pipelineBuilders, AbstractChainStage<?> currentChainStage) {
        this.pipelineBuilder = pipelineBuilder;
        this.otherPipelineBuilders = pipelineBuilders;
        this.currentChainStage = currentChainStage;
    }

    /**
     * 选择一个分支
     *
     * @param lableName
     * @return
     */
    public DataStream select(String lableName) {
        StageBuilder operator = new StageBuilder() {
            @Override protected IMessage handleMessage(IMessage message, AbstractContext context) {
                System.out.println(message.getMessageBody());
                return null;
            }

        };

        AbstractChainStage<?> stage = this.pipelineBuilder.createStage(operator);
        stage.setLabel(lableName);
        this.pipelineBuilder.setTopologyStages(currentChainStage, stage);
        return new DataStream(pipelineBuilder, otherPipelineBuilders, stage);
    }

    public DataStream toDataStream() {
        return new DataStream(pipelineBuilder, otherPipelineBuilders, currentChainStage);
    }

}
