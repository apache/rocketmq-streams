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
package org.apache.rocketmq.streams.filter.operator.action.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.IChannel;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.filter.context.RuleContext;
import org.apache.rocketmq.streams.filter.operator.Rule;
import org.apache.rocketmq.streams.filter.operator.action.Action;

public class ChannelAction extends Action<Boolean> implements IAfterConfiguableRefreshListerner {

    private static final Log LOG = LogFactory.getLog(ChannelAction.class);
    protected String channelName;
    protected transient IChannel channel;

    public ChannelAction() {
        setType(Action.TYPE);
    }

    public ChannelAction setChannel(IChannel channel) {
        this.channelName = channel.getConfigureName();
        this.channel = channel;
        return this;
    }

    @Override
    public Boolean doAction(RuleContext context, Rule rule) {
        channel.batchAdd(context.getMessage());
        return true;
    }

    @Override
    public boolean volidate(RuleContext context, Rule rule) {
        return true;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        IChannel channel = configurableService.queryConfigurable(IChannel.TYPE, channelName);
        channel.openAutoFlush();
        this.channel = channel;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }
}
