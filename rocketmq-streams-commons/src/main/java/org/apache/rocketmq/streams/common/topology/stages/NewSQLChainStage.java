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
package org.apache.rocketmq.streams.common.topology.stages;

import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;

public class NewSQLChainStage<T extends IMessage> extends AbstractStatelessChainStage<T> implements IAfterConfiguableRefreshListerner {
    protected String sqlMessageProcessorName;
    protected transient IStreamOperator<IMessage, IMessage> messageProcessor;

    public NewSQLChainStage() {
        setEntityName("SQL");
    }

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            IMessage msg = messageProcessor.doMessage(message, context);
            message.setMessageBody(msg.getMessageBody());
            context.setMessage(message);
            return message;
        }

        @Override
        public String getName() {
            return NewSQLChainStage.class.getName();
        }
    };

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        this.messageProcessor = configurableService.<IStreamOperator>queryConfigurable(IStreamOperator.TYPE,
            sqlMessageProcessorName);
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    public String getSqlMessageProcessorName() {
        return sqlMessageProcessorName;
    }

    public void setSqlMessageProcessorName(String sqlMessageProcessorName) {
        this.sqlMessageProcessorName = sqlMessageProcessorName;
    }

    public void setMessageProcessor(IStreamOperator messageProcessor) {
        this.messageProcessor = messageProcessor;
        if (IConfigurable.class.isInstance(messageProcessor)) {
            IConfigurable configurable = (IConfigurable)messageProcessor;
            this.sqlMessageProcessorName = configurable.getConfigureName();
        }

    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }
}
