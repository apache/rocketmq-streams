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

import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;

public class PythonChainStage<T extends IMessage> extends AbstractStatelessChainStage<T> implements IAfterConfigurableRefreshListener {
    protected String pythonChannelName;
    protected transient IChannel channel;
    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            IStreamOperator<IMessage, IMessage> receiver = (IStreamOperator)channel;
            IMessage msg = receiver.doMessage(message, context);
            message.setMessageBody(msg.getMessageBody());
            context.setMessage(message);
            return message;
        }

        @Override
        public String getName() {
            return PythonChainStage.class.getName();
        }
    };

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        IChannel channel = configurableService.queryConfigurable(IChannel.TYPE, pythonChannelName);
        this.channel = channel;
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    public String getPythonChannelName() {
        return pythonChannelName;
    }

    public void setPythonChannelName(String pythonChannelName) {
        this.pythonChannelName = pythonChannelName;
    }

    public IChannel getChannel() {
        return channel;
    }

    public void setChannel(IChannel channel) {
        this.channel = channel;
        this.setNameSpace(channel.getNameSpace());
        this.setPythonChannelName(channel.getConfigureName());
    }

    @Override
    public boolean isAsyncNode() {
        return false;
    }
}
