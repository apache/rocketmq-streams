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

import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.topology.stages.udf.UDFUnionChainStage;

public class Union extends BasedConfigurable implements IStreamOperator<IMessage, AbstractContext<IMessage>>,
    IStageBuilder<AbstractChainStage<?>> {
    public static String TYPE = "union";
    protected transient IStreamOperator<IMessage, AbstractContext<IMessage>> receiver;

    public Union() {
        setType(TYPE);
    }

    public IStreamOperator<IMessage, AbstractContext<IMessage>> getReceiver() {
        return receiver;
    }

    public void setReceiver(
        IStreamOperator<IMessage, AbstractContext<IMessage>> receiver) {
        this.receiver = receiver;
    }

    @Override
    public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
        return receiver.doMessage(message, context);
    }

    @Override
    public AbstractChainStage createStageChain(PipelineBuilder pipelineBuilder) {
        pipelineBuilder.addConfigurables(this);
        UDFUnionChainStage chainStage = new UDFUnionChainStage();
        chainStage.setUnion(this);
        return chainStage;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {

    }
}
