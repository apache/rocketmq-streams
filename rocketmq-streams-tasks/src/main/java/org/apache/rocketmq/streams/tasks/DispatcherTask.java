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
package org.apache.rocketmq.streams.tasks;

import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configure.ConfigureFileKey;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.RocketmqDispatcher;
import org.apache.rocketmq.streams.tasks.cache.DBCache;
import org.apache.rocketmq.streams.tasks.callback.DispatcherCallback;

public class DispatcherTask extends StreamTask {

    private transient String dispatcherGroup;

    private transient RocketmqDispatcher<?> dispatcher;

    private transient DispatcherCallback balanceCallback;

    public DispatcherTask() {
    }

    public DispatcherTask(String namespace, String configName) {
        this.setNameSpace(namespace);
        this.setConfigureName(configName);
    }

    public DispatcherTask(String namespace, String configName, String dispatcherGroup) {
        this(namespace, configName);
        this.dispatcherGroup = dispatcherGroup;
    }

    @Override public void start() {
        try {
            ConfigurableComponent configurableComponent = ComponentCreator.getComponent(this.getNameSpace(), ConfigurableComponent.class);
            if (this.balanceCallback == null) {
                this.balanceCallback = new DispatcherCallback(this.getNameSpace(), this.getConfigureName(), configurableComponent);
            }
            if (this.dispatcher == null) {
                //根据不同的调度系统，构建不同的Dispatcher
                String dispatcherType = configurableComponent.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_TYPE, "rocketmq");
                if (dispatcherType.equalsIgnoreCase("rocketmq")) {
                    String nameServAddr = configurableComponent.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_NAMESERV, "127.0.0.1:9876");
                    String voteTopic = configurableComponent.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_TOPIC, "VOTE_TOPIC");
                    if (this.dispatcherGroup == null) {
                        this.dispatcherGroup = configurableComponent.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_ROCKETMQ_GROUP, getConfigureName());
                    }
                    String dispatchMode = configurableComponent.getProperties().getProperty(ConfigureFileKey.DIPPER_DISPATCHER_MODE, DispatchMode.LEAST.getName());
                    this.dispatcher = new RocketmqDispatcher<>(nameServAddr, voteTopic, IdUtil.instanceId(), this.dispatcherGroup, DispatchMode.valueOf(dispatchMode), this.balanceCallback, new DBCache());
                }
                this.dispatcher.start();
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void destroy() {
        if (this.dispatcher != null) {
            this.dispatcher.close();
        }
    }

    public ChainPipeline<?> getPipeline(String taskName) {
        return this.configurableService.queryConfigurable(ChainPipeline.TYPE, taskName);
    }

    public DispatcherCallback getBalanceCallback() {
        return balanceCallback;
    }

    public void setBalanceCallback(DispatcherCallback balanceCallback) {
        this.balanceCallback = balanceCallback;
    }

    public RocketmqDispatcher<?> getDispatcher() {
        return dispatcher;
    }

    public String getDispatcherGroup() {
        return dispatcherGroup;
    }

    public void setDispatcherGroup(String dispatcherGroup) {
        this.dispatcherGroup = dispatcherGroup;
    }
}
