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
package org.apache.rocketmq.streams.tasks.callback;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.constant.State;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.assigner.TaskAssigner;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
import org.apache.rocketmq.streams.configurable.ConfigurableComponent;
import org.apache.rocketmq.streams.dispatcher.IDispatcherCallback;
import org.apache.rocketmq.streams.dispatcher.constant.DispatcherConstant;
import org.apache.rocketmq.streams.tasks.entity.WeightChainPipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatcherCallback implements IDispatcherCallback {

    private static final Logger LOGGER = LoggerFactory.getLogger(DispatcherCallback.class);

    private transient ConfigurableComponent configurableComponent;

    private final Map<String, ChainPipeline<?>> onlinePipelines = Maps.newHashMap();

    private Map<String, ChainPipeline<?>> dispatchPipelines = Maps.newHashMap();

    private Map<String, ChainPipeline<?>> dispatchOriginPipelines = Maps.newHashMap();

    private String namespace;
    private String configureName;

    private final transient String instanceId;

    public DispatcherCallback(String namepace, String configureName, ConfigurableComponent configurableComponent) {
        this.namespace = namepace;
        this.configureName = configureName;
        this.configurableComponent = configurableComponent;
        this.instanceId = IdUtil.instanceId();
    }

    @Override public List<String> start(List<String> names) {
        List<String> startSuccess = Lists.newArrayList();
        for (String name : names) {
            if (!this.onlinePipelines.containsKey(name)) {
                ChainPipeline<?> pipeline = dispatchPipelines.get(name);
                if (pipeline != null && pipeline.getState().equals(State.STARTED)) {
                    try {
                        pipeline.startChannel();
                        startSuccess.add(name);
                        onlinePipelines.put(name, pipeline);
                        LOGGER.info("[{}][{}] Task_Start_Success", this.instanceId, name);
                    } catch (Exception e) {
                        startSuccess.remove(name);
                        onlinePipelines.remove(name);
                        LOGGER.error("[{}][{}] Task_Start_Error", this.instanceId, name, e);
                    }
                } else {
                    LOGGER.error("[{}][{}] Task_Start_Error_Task_Not_Found", this.instanceId, name);
                }
            }
        }
        return startSuccess;
    }

    @Override public List<String> stop(List<String> names) {
        List<String> stopSuccess = Lists.newArrayList();
        for (String name : names) {
            if (this.onlinePipelines.containsKey(name)) {
                ChainPipeline<?> pipeline = this.onlinePipelines.get(name);
                try {
                    pipeline.destroy();
                    stopSuccess.add(name);
                    this.onlinePipelines.remove(name);
                    LOGGER.info("[{}][{}] Task_Stop_Success", this.instanceId, name);
                } catch (Exception e) {
                    stopSuccess.remove(name);
                    this.onlinePipelines.put(name, pipeline);
                    LOGGER.error("[{}][{}] Task_Stop_Error", this.instanceId, name, e);
                }
            }
        }
        return stopSuccess;
    }

    /**
     * 仅调度状态为start的任务
     *
     * @return 返回状态为start的
     */
    @Override public List<String> list(List<String> instanceIdList) {
        Map<String, ChainPipeline<?>> dispatchPipelines = Maps.newHashMap();
        Map<String, ChainPipeline<?>> dispatchOriginPipelines = Maps.newHashMap();
        List<WeightChainPipeline> allPipelines = loadSubPipelines();
        for (WeightChainPipeline weightChainPipeline : allPipelines) {
            ChainPipeline<?> chainPipeline = weightChainPipeline.getChainPipeline();
            if (chainPipeline.getState().equals(State.STARTED)) {
                int weight = weightChainPipeline.getWeight();
                if (weight > instanceIdList.size()) {
                    weight = instanceIdList.size();
                }
                for (int i = 0; i < weight; i++) {
                    String configureName = chainPipeline.getConfigureName() + DispatcherConstant.TASK_SERIAL_NUM_SEPARATOR + i;
                    if (!this.dispatchOriginPipelines.containsKey(configureName) || !this.dispatchOriginPipelines.get(configureName).equals(chainPipeline)) {
                        ChainPipeline<?> clone = clone(configureName, chainPipeline);
                        dispatchPipelines.put(configureName, clone);
                        dispatchOriginPipelines.put(configureName, chainPipeline);
                        if (this.onlinePipelines.containsKey(configureName)) {
                            try {
                                this.onlinePipelines.get(configureName).destroy();
                                clone.startChannel();
                                this.onlinePipelines.put(configureName, clone);
                            } catch (Exception e) {
                                LOGGER.error("[{}][{}] Hot_Update_Error", IdUtil.instanceId(), configureName, e);
                            }
                        }
                    } else {
                        dispatchPipelines.put(configureName, this.dispatchPipelines.get(configureName));
                        dispatchOriginPipelines.put(configureName, this.dispatchOriginPipelines.get(configureName));
                    }
                }
            }
        }
        this.dispatchPipelines = dispatchPipelines;
        this.dispatchOriginPipelines = dispatchOriginPipelines;
        return Lists.newArrayList(this.dispatchPipelines.keySet());
    }

    private ChainPipeline<?> clone(String configureName, ChainPipeline<?> chainPipeline) {
        ChainPipeline<?> copy = ReflectUtil.forInstance(chainPipeline.getClass());
        copy.toObject(chainPipeline.toJson());
        copy.setConfigureName(configureName);
        copy.setStages(chainPipeline.getStages());
        copy.setStageGroups(chainPipeline.getStageGroups());
        copy.setStageMap(chainPipeline.getStageMap());
        copy.setChannelNextStageLabel(chainPipeline.getChannelNextStageLabel());
        copy.setSource(chainPipeline.getSource());
        copy.setRootStageGroups(chainPipeline.getRootStageGroups());
        copy.setState(chainPipeline.getState());
        return copy;
    }

    public List<WeightChainPipeline> loadSubPipelines() {
        List<TaskAssigner> taskAssigners = this.configurableComponent.queryConfigurableByType(TaskAssigner.TYPE);
        if (taskAssigners == null) {
            return null;
        }
        List<WeightChainPipeline> weightChainPipelines = Lists.newArrayList();
        for (TaskAssigner taskAssigner : taskAssigners) {
            if (!this.configureName.equals(taskAssigner.getTaskName())) {
                continue;
            }
            String pipelineName = taskAssigner.getPipelineName();
            Integer weight = taskAssigner.getWeight();
            if (pipelineName != null) {
                ChainPipeline<?> pipeline = this.configurableComponent.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    WeightChainPipeline weightChainPipeline = new WeightChainPipeline();
                    weightChainPipeline.setChainPipeline(pipeline);
                    weightChainPipeline.setWeight(weight);
                    weightChainPipelines.add(weightChainPipeline);
                }
            }
        }
        return weightChainPipelines;
    }

    public String getConfigureName() {
        return configureName;
    }

    public void setConfigureName(String configureName) {
        this.configureName = configureName;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public ConfigurableComponent getConfigurableComponent() {
        return configurableComponent;
    }

    public Map<String, ChainPipeline<?>> getOnlinePipelines() {
        return onlinePipelines;
    }

    public Map<String, ChainPipeline<?>> getDispatchPipelines() {
        return dispatchPipelines;
    }
}
