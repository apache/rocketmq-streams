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
package org.apache.rocketmq.streams.common.topology.graph;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.Properties;
import org.apache.rocketmq.streams.common.configuration.SystemContext;
import org.apache.rocketmq.streams.common.topology.IJobGraph;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;

public class JobGraph implements IJobGraph {

    private String namespace;
    private String jobName;
    private List<ChainPipeline<?>> pipelines;
    private Properties jobConfiguration;

    public JobGraph(String namespace, String jobName, List<ChainPipeline<?>> pipelines) {
        this.namespace = namespace;
        this.jobName = jobName;
        this.pipelines = pipelines;
        this.jobConfiguration = SystemContext.getProperties();
    }

    public JobGraph(String namespace, String jobName, List<ChainPipeline<?>> pipelines, Properties jobConfiguration) {
        this.namespace = namespace;
        this.jobName = jobName;
        this.pipelines = pipelines;
        this.jobConfiguration = jobConfiguration;
    }

    @Override public void start() {
        for (ChainPipeline<?> pipeline : pipelines) {
            pipeline.startJob();
        }
    }

    @Override public void stop() {
        for (ChainPipeline<?> pipeline : pipelines) {
            pipeline.destroy();
        }
    }

    @Override public List<JSONObject> execute(List<JSONObject> dataList) {
        return pipelines.get(0).executeWithMsgs(dataList);
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public List<ChainPipeline<?>> getPipelines() {
        return pipelines;
    }

    public void setPipelines(List<ChainPipeline<?>> pipelines) {
        this.pipelines = pipelines;
    }

    public Properties getJobConfiguration() {
        return jobConfiguration;
    }

    public void setJobConfiguration(Properties jobConfiguration) {
        this.jobConfiguration = jobConfiguration;
    }

}
