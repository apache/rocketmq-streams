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
package org.apache.rocketmq.streams.common.topology.task;

import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.cache.compress.BitSetCache;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.MessageGlobleTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintMetric;
import org.apache.rocketmq.streams.common.threadpool.ThreadPoolFactory;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * run one or multi pipeline's
 */
public class StreamsTask extends BasedConfigurable implements IAfterConfigurableRefreshListener {
    private static final Log LOG = LogFactory.getLog(StreamsTask.class);

    public static final String TYPE = "stream_task";




    /**
     * 任务的状态，目前有started，stopped俩种， 任务序列化保存在数据库
     */
    protected String state = "stopped";
    /**
     * 在当前进程中任务的状态
     */
    protected transient AtomicBoolean isStarted = new AtomicBoolean(false);
    /**
     * The pipeline or subtask executed in this task
     */
    protected transient List<ChainPipeline<?>> pipelines = new ArrayList<>();
    protected List<String> pipelineNames = new ArrayList<>();









    public StreamsTask() {
        setType(TYPE);
    }

    public void start() {
        if (this.isStarted.compareAndSet(false, true)) {
            for (ChainPipeline<?> pipeline : pipelines) {
                startPipeline(pipeline);
            }
        }
    }

    @Override public void destroy() {
        for (ChainPipeline<?> pipeline : pipelines) {
            pipeline.destroy();
        }
    }


    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {

        List<ChainPipeline<?>> newPipelines = new ArrayList<>();
        boolean isChanged = false;

        if (this.pipelineNames != null && !pipelineNames.isEmpty()) {
            for (String pipelineName : this.pipelineNames) {
                ChainPipeline<?> pipeline = configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
                if (pipeline != null) {
                    newPipelines.add(pipeline);
                }
            }
        }

        List<ChainPipeline<?>> deletePipeline = new ArrayList<>();
        if (newPipelines.size() > 0) {
            for (ChainPipeline<?> pipeline : newPipelines) {
                if (!this.pipelines.contains(pipeline)) {
                    isChanged = true;
                    break;
                }
            }
            for (ChainPipeline<?> pipeline : this.pipelines) {
                if (!newPipelines.contains(pipeline)) {
                    isChanged = true;
                    deletePipeline.add(pipeline);
                }
            }
        }

        if (isChanged) {
            this.pipelines = newPipelines;
            for (ChainPipeline<?> pipeline : deletePipeline) {
                pipeline.destroy();
            }
        }

    }







    /**
     * start one pipeline
     *
     * @param pipeline pipeline
     */
    protected void startPipeline(ChainPipeline<?> pipeline) {
        Thread thread = new Thread(pipeline::startChannel);
        thread.start();
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



    public List<String> getPipelineNames() {
        return pipelineNames;
    }

    public void setPipelineNames(List<String> pipelineNames) {
        this.pipelineNames = pipelineNames;
    }

    public AtomicBoolean getIsStarted() {
        return isStarted;
    }

    public void setIsStarted(AtomicBoolean isStarted) {
        this.isStarted = isStarted;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }
}
