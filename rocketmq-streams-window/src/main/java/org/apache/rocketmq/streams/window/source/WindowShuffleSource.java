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
package org.apache.rocketmq.streams.window.source;

import org.apache.rocketmq.streams.common.channel.source.AbstractSource;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.SectionPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.window.operator.AbstractWindow;
import org.apache.rocketmq.streams.window.util.WindowChannellUtil;

public class WindowShuffleSource extends AbstractSource implements IAfterConfigurableRefreshListener {

    protected String windowName;
    protected String pipelineName;
    protected String windowStageLableName;

    protected transient AbstractSource source;
    protected transient AbstractWindow window;
    protected transient IStreamOperator pipelineAfterWindow;

    @Override protected boolean startSource() {
        source.start(pipelineAfterWindow);
        return true;
    }

    public String getWindowName() {
        return windowName;
    }

    public void setWindowName(String windowName) {
        this.windowName = windowName;
    }

    public void setWindow(AbstractWindow window) {
        this.window = window;
        this.windowName = window.getConfigureName();
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        window = configurableService.queryConfigurable(IWindow.TYPE, windowName);
        ChainPipeline pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
        AbstractStage windowStage = (AbstractStage) pipeline.getStageMap().get(windowStageLableName);
        pipelineAfterWindow = new SectionPipeline(pipeline, windowStage);
        String connector = ComponentCreator.getProperties().getProperty(WindowChannellUtil.WINDOW_SHUFFLE_CHANNEL_TYPE);
        source = (AbstractSource) WindowChannellUtil.createSource(window.getNameSpace(), window.getConfigureName(), connector, ComponentCreator.getProperties(), WindowChannellUtil.WINDOW_SHUFFLE_CHANNEL_PROPERTY_PREFIX, getDynamicPropertyValue());
        if (source == null) {

        }
    }

    protected String getDynamicPropertyValue() {
        String dynamicPropertyValue = MapKeyUtil.createKey(window.getNameSpace(), window.getConfigureName());
        dynamicPropertyValue = dynamicPropertyValue.replaceAll("\\.", "_").replaceAll(";", "_");
        return dynamicPropertyValue;
    }

    @Override public boolean supportRemoveSplitFind() {
        throw new RuntimeException("can not support this method");
    }

    @Override protected boolean isNotDataSplit(String queueId) {
        throw new RuntimeException("can not support this method");
    }

}
