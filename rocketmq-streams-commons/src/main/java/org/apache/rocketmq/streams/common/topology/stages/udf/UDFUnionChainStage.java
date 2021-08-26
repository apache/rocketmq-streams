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

import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.Union;
import org.apache.rocketmq.streams.common.topology.stages.AbstractStatelessChainStage;

public class UDFUnionChainStage extends AbstractStatelessChainStage implements IAfterConfigurableRefreshListener {
    protected String unionName;//union对象的名字
    protected boolean isMainStream = false;//是主流，在union时，主流.union(支流)

    protected transient Union union;

    @Override
    public boolean isAsyncNode() {
        return false;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        union = configurableService.queryConfigurable(Union.TYPE, unionName);
        if (isMainStream) {
            IStreamOperator<IMessage, AbstractContext<IMessage>> receiver = getReceiverAfterCurrentNode();
            union.setReceiver(receiver);
        }

    }

    @Override
    protected IStageHandle selectHandle(IMessage message, AbstractContext context) {
        return new IStageHandle() {
            @Override
            protected IMessage doProcess(IMessage message, AbstractContext context) {
                if (!isMainStream) {
                    union.doMessage(message, context);
                }
                return message;
            }

            @Override
            public String getName() {
                return UDFUnionChainStage.class.getName();
            }
        };
    }

    public void setUnion(Union union) {
        this.union = union;
        if (union != null) {
            setUnionName(union.getConfigureName());
            setLabel(union.getConfigureName());
        }
    }

    public String getUnionName() {
        return unionName;
    }

    public void setUnionName(String unionName) {
        this.unionName = unionName;
    }

    public boolean isMainStream() {
        return isMainStream;
    }

    public void setMainStream(boolean mainStream) {
        isMainStream = mainStream;
    }
}
