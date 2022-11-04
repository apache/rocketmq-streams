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

import java.util.List;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.enums.DispatchMode;
import org.apache.rocketmq.streams.dispatcher.impl.RocketmqDispatcher;
import org.apache.rocketmq.streams.tasks.callback.DispatcherCallback;

public class AggregatedTask extends StreamTask {

    protected transient IDispatcherCallback balanceCallback;
    private final transient RocketmqDispatcher<?> taskDispatcher;

    public AggregatedTask(String nameServAddr, String instanceName, String configName) {
        this.setConfigureName(configName);
        ConfigurableComponent configurableComponent = ComponentCreator.getComponent(this.getNameSpace(), ConfigurableComponent.class);
        this.balanceCallback = new DispatcherCallback(this.getNameSpace(), this.getConfigureName(), configurableComponent);
        this.taskDispatcher = new RocketmqDispatcher<>(nameServAddr, "aggrgate_topic", instanceName, configName, DispatchMode.AVERAGELY, balanceCallback);
    }

    @Override public void start() {
        try {
            taskDispatcher.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<ChainPipeline<?>> pipelines = loadSubPipelines();
        boolean isChanged = checkPipelineChanged(pipelines, false);
        if (isChanged) {
            try {
                taskDispatcher.wakeUp();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }

    private ChainPipeline<?> getPipeline(String taskName) {
        return this.configurableService.queryConfigurable(ChainPipeline.TYPE, taskName);
    }

}
