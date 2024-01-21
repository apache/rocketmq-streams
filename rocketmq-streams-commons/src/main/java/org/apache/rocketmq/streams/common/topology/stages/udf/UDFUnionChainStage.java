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

import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.model.Union;
import org.apache.rocketmq.streams.common.topology.stages.AbstractStatelessChainStage;

public class UDFUnionChainStage extends AbstractStatelessChainStage {
    protected boolean isMainStream = false;//是主流，在union时，主流.union(支流)

    @ConfigurableReference protected Union union;

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    protected IMessage handleMessage(IMessage message, AbstractContext context) {
        if (!isMainStream) {
            union.doMessage(message, context);
        }
        return message;
    }

    public void setUnion(Union union) {
        this.union = union;
        if (union != null) {
            setLabel(union.getName());
        }
    }

    @Override public void startJob() {
        super.startJob();
        if (isMainStream) {
            IStreamOperator<IMessage, AbstractContext<IMessage>> receiver = getReceiverAfterCurrentNode();
            union.setReceiver(receiver);
        }
    }

    public boolean isMainStream() {
        return isMainStream;
    }

    public void setMainStream(boolean mainStream) {
        isMainStream = mainStream;
    }
}
