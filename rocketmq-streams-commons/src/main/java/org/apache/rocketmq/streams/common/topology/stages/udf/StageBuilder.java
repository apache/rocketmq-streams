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
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.stages.AbstractStatelessChainStage;

/**
 * 给用户提供自定义的抽象类
 */
public abstract class StageBuilder extends AbstractStatelessChainStage<IMessage>
    implements IStageBuilder<ChainStage>, Serializable, IAfterConfiguableRefreshListerner {

    @Override
    protected boolean initConfigurable() {

        return true;
    }

    /**
     * 子类实现，实现具体的处理逻辑
     *
     * @param message message
     * @param context context
     * @param <T>
     * @return
     */
    protected abstract <T> T operate(IMessage message, AbstractContext context);

    @Override
    protected IStageHandle selectHandle(IMessage message, AbstractContext context) {
        return new IStageHandle() {
            @Override
            protected IMessage doProcess(IMessage message, AbstractContext context) {
                operate(message, context);
                return message;
            }

            @Override
            public String getName() {
                return StageBuilder.class.getName();
            }
        };
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    public ChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        return new UDFChainStage(this);
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {

    }

    public static void main(String[] args) {

    }
}
