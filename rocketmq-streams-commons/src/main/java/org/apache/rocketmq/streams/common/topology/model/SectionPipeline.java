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
package org.apache.rocketmq.streams.common.topology.model;

import java.util.List;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;

/**
 * A part of the pipeline, starting from a stage and going back
 */
public class SectionPipeline implements IStreamOperator<IMessage, AbstractContext<IMessage>>, IConfigurableIdentification {
    protected ChainPipeline pipeline;
    protected AbstractStage currentStage;

    public SectionPipeline(AbstractPipeline pipeline, AbstractStage currentStage) {
        this((ChainPipeline) pipeline, currentStage);
    }

    public SectionPipeline(ChainPipeline pipeline, AbstractStage currentStage) {
        this.pipeline = pipeline;
        this.currentStage = currentStage;
    }

    @Override public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
        //设置window触发后需要执行的逻辑
        if (pipeline.isTopology()) {
            pipeline.doNextStages(context, currentStage.getMsgSourceName(), currentStage.getLabel(), currentStage.getNextStageLabels());
            return context;

        } else {
            throw new RuntimeException("expect Topology, but not " + pipeline.getName());
        }
    }

    /**
     * 选择当前stage所在的位置
     *
     * @param pipeLine
     * @return
     */
    protected int chooseWindowStageNextIndex(ChainPipeline pipeLine) {
        List<AbstractStage<?>> stages = pipeLine.getStages();
        for (int i = 0; i < stages.size(); i++) {
            AbstractStage stage = stages.get(i);
            if (isCurrentWindowStage(stage)) {
                return (i + 1);
            }
        }
        return -1;
    }

    /**
     * 判断stage是否是当前stage
     *
     * @param stage
     * @return
     */

    protected boolean isCurrentWindowStage(AbstractStage stage) {
        return this.currentStage.equals(stage);
    }

    public ChainPipeline getPipeline() {
        return pipeline;
    }

    @Override public String getName() {
        return pipeline.getName();
    }

    @Override public String getNameSpace() {
        return pipeline.getNameSpace();
    }

    @Override public String getType() {
        return pipeline.getType();
    }

}
