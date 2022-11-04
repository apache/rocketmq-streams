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

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.interfaces.IConfigurableLifeStart;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.assigner.TaskAssigner;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * run one or multi pipeline's
 */
public class StreamTask extends BasedConfigurable implements IAfterConfigurableRefreshListener, IConfigurableLifeStart {
    private static final Logger LOGGER = LoggerFactory.getLogger(StreamTask.class);

    public static final String TYPE = "stream_task";
    /**
     * 在当前进程中任务的状态
     */
    protected transient Map<ChainPipeline<?>, Boolean> pipelineHasStart = new HashMap<>();
    /**
     * The pipeline or subtask executed in this task
     */
    protected transient List<ChainPipeline<?>> pipelines;
    protected List<String> pipelineNames = new ArrayList<>();

    public StreamTask() {
        setType(TYPE);
    }

    @Override public void start() {
        for (ChainPipeline<?> pipeline : pipelines) {
            if (!pipelineHasStart.containsKey(pipeline)) {
                startPipeline(pipeline);
                pipelineHasStart.put(pipeline, true);
            }
        }

    }

    @Override public void destroy() {
        for (ChainPipeline<?> pipeline : pipelines) {
            pipeline.destroy();
            pipelineHasStart.remove(pipeline);
        }
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        List<ChainPipeline<?>> newPipelines = new ArrayList<>();
        if (this.pipelineNames != null && !pipelineNames.isEmpty()) {
            for (String pipelineName : this.pipelineNames) {
                ChainPipeline<?> pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    newPipelines.add(pipeline);
                }
            }
        }
        checkPipelineChanged(newPipelines, true);
    }

    protected boolean checkPipelineChanged(List<ChainPipeline<?>> newPipelines, boolean destroy) {
        boolean isChanged = false;
        if (this.pipelines == null) {
            this.pipelines = new ArrayList<>(newPipelines);
        }
        List<ChainPipeline<?>> needStartedPipelines = Lists.newArrayList();
        List<ChainPipeline<?>> needDestroyPipelines = Lists.newArrayList();
        if (newPipelines.size() > 0) {
            for (ChainPipeline<?> pipeline : newPipelines) {
                if (!this.pipelines.contains(pipeline)) {
                    needStartedPipelines.add(pipeline);
                }
            }
            for (ChainPipeline<?> pipeline : this.pipelines) {
                if (!newPipelines.contains(pipeline)) {
                    needDestroyPipelines.add(pipeline);
                }
            }
        }
        if (!needStartedPipelines.isEmpty() || !needDestroyPipelines.isEmpty()) {
            LOGGER.info("[{}][{}] Pipeline_Changed_NeedStart({})_NeedDestroy({})", IdUtil.instanceId(), getConfigureName(), JSONObject.toJSONString(needStartedPipelines), JSONObject.toJSONString(needDestroyPipelines));
            isChanged = true;
        }

        if (isChanged) {
            this.pipelines = newPipelines;
            if (destroy) {
                for (ChainPipeline<?> pipeline : needDestroyPipelines) {
                    try {
                        pipeline.destroy();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }
        return isChanged;
    }

    /**
     * start one pipeline
     *
     * @param pipeline pipeline
     */
    public void startPipeline(ChainPipeline<?> pipeline) {
        try {
            pipeline.startChannel();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<ChainPipeline<?>> getPipelines() {
        return pipelines;
    }

    public void setPipelines(List<ChainPipeline<?>> pipelines) {
        this.pipelines = pipelines;
        List<String> pipelineNames = Lists.newArrayList();
        for (ChainPipeline<?> pipeline : this.pipelines) {
            pipelineNames.add(pipeline.getConfigureName());
        }
        this.pipelineNames = pipelineNames;
    }

    /**
     * 动态装配子pipeline
     */
    public List<ChainPipeline<?>> loadSubPipelines() {
        List<TaskAssigner> taskAssigners = this.configurableService.queryConfigurableByType(TaskAssigner.TYPE);
        if (taskAssigners == null) {
            return null;
        }
        String taskName = getConfigureName();
        List<ChainPipeline<?>> subPipelines = Lists.newArrayList();
        for (TaskAssigner taskAssigner : taskAssigners) {
            if (!taskName.equals(taskAssigner.getTaskName())) {
                continue;
            }
            String pipelineName = taskAssigner.getPipelineName();
            if (pipelineName != null) {
                ChainPipeline<?> pipeline = this.configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    subPipelines.add(pipeline);
                }
            }
        }
        return subPipelines;
    }

    public List<String> getPipelineNames() {
        return pipelineNames;
    }

    public void setPipelineNames(List<String> pipelineNames) {
        this.pipelineNames = pipelineNames;
    }

}
