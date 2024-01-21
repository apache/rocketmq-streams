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
package org.apache.rocketmq.streams.common.topology.stages.udf;

import java.io.Serializable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.stages.AbstractStatelessChainStage;

/**
 * 给用户提供自定义的抽象类
 */
public abstract class StageBuilder extends AbstractStatelessChainStage<IMessage> implements IStageBuilder<AbstractChainStage<?>>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    protected boolean initConfigurable() {
        return true;
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    public AbstractChainStage<?> createStageChain(PipelineBuilder pipelineBuilder) {
        return new UDFChainStage(this);
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }

}
