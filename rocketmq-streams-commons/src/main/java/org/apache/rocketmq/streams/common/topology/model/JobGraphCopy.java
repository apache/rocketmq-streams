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
package org.apache.rocketmq.streams.common.topology.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.enums.StageType;
import org.apache.rocketmq.streams.common.model.JobConfigure;
import org.apache.rocketmq.streams.common.model.JobStage;
import org.apache.rocketmq.streams.common.model.StageRelation;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

public class JobGraphCopy extends BasedConfigurable {
    public static final String DEFAULT_NAMESPACE = "dipper.private.blink.rules";
    public static String TYPE = "JobGraph";

    protected transient List<String> pipelineNamesBySource;
    protected transient List<ChainPipeline<?>> pipelinesBySource;//一个数据源一个pipeline

    /**
     * 为了监控使用，正常使用可以暂时忽略
     */
    protected List<JobConfigure> jobConfigures;
    protected List<StageRelation> stageRelations;
    protected List<JobStage> jobStages;

    public JobGraphCopy() {
        setType(TYPE);
    }

    private static void deepBuild(List<StageGroup> children, List<StageRelation> stageRelationDOList, String jobName) {
        if (CollectionUtils.isEmpty(children)) {
            return;
        }
        for (StageGroup child : children) {
            StageRelation stageRelationDO = buildStageRelation(child, jobName);
            stageRelationDOList.add(stageRelationDO);
            deepBuild(child.getChildren(), stageRelationDOList, jobName);
        }
    }

    private static StageRelation buildStageRelation(StageGroup stageGroup, String jobName) {
        // TODO group中需要加入pos信息
        String sqlContent = new JSONObject()
            .fluentPut("sql", stageGroup.getSql())
            //  .fluentPut("pos", stageGroup.getPos())
            .toJSONString();
        StageRelation stageRelation = new StageRelation();
        stageRelation.setJobName(jobName);
        stageRelation.setGroupName(stageGroup.getName());
        stageRelation.setSqlContent(sqlContent);
        stageRelation.setViewName(stageGroup.getViewName());
        stageRelation.setStageLabels(JSON.toJSONString(stageGroup.getAllStageLabels()));
        stageRelation.setStartLabel(stageGroup.getStartLabel());
        stageRelation.setEndLabel(stageGroup.getEndLabel());
        stageRelation.setParentName(stageGroup.getParentName());
        stageRelation.setChildrenNames(JSON.toJSONString(stageGroup.getChildrenNames()));
        return stageRelation;
    }

    private static String getSqlContent(AbstractStage stage) {
        return new JSONObject()
            .fluentPut("sql", stage.getSql())
            //  .fluentPut("pos", stage.getPos())
            .toJSONString();
    }

    protected static String getStageContent(AbstractStage stage) {
        String type = StageType.getTypeForStage(stage);
        StringBuilder sb = new StringBuilder();
        switch (type) {
            case "filter":
                FilterChainStage filterChainStage = (FilterChainStage) stage;
                AbstractRule rule = filterChainStage.getRule();
                sb.append(rule.toString() + PrintUtil.LINE);
                break;
            case "script":
                ScriptChainStage scriptChainStage = (ScriptChainStage) stage;
                AbstractScript functionScript = scriptChainStage.getScript();
                functionScript.init();
                sb.append(functionScript.toString());
                break;
        }
        return sb.toString();
    }

    /**
     * 创建监控相关的信息
     *
     * @param pipelines
     * @param configurables
     */
    public void createJobInfo(List<ChainPipeline<?>> pipelines, List<IConfigurable> configurables) {
        createJobConfiguables(configurables);
        createJobStages(pipelines);
        createStageRelation(pipelines);
    }

    private void createJobStages(List<ChainPipeline<?>> pipelines) {
        if (pipelines == null) {
            return;
        }
        for (ChainPipeline pipeline : pipelines) {
            List<JobStage> stageDOList = new ArrayList<>();
            int i = 0;
            List<AbstractStage<?>> stages = pipeline.getStages();
            for (AbstractStage<?> stage : stages) {
                List<String> prevStageLabels = stage.getPrevStageLabels();
                if (CollectionUtils.isEmpty(prevStageLabels) || StringUtils.equals(prevStageLabels.get(0), getName())) {
                    //上游为null 虚拟一个source
                    String sqlContent = new JSONObject()
                        .fluentPut("sql", pipeline.getCreateTableSQL())
                        //  .fluentPut("pos", pipeline.getCreateTableSQLPos())
                        .toJSONString();
                    JobStage source = new JobStage();
                    source.setJobName(getName());
                    source.setStageName(getName() + "_source_" + i++);
                    source.setMachineName("");
                    source.setStageType(StageType.SOURCE.getType());
                    source.setStageContent("");
                    source.setSqlContent(sqlContent);
                    source.setPrevStageLables("[]");
                    source.setNextStageLables("[\"" + stage.getLabel() + "\"]");
                    stageDOList.add(source);
                }
                JobStage stageDO = new JobStage();
                stageDO.setJobName(getName());
                stageDO.setStageName(stage.getLabel());
                stageDO.setMachineName("");
                stageDO.setStageType(StageType.getTypeForStage(stage));
                stageDO.setStageContent(getStageContent(stage));
                stageDO.setSqlContent(getSqlContent(stage));
                stageDO.setPrevStageLables(JSON.toJSONString(prevStageLabels));
                stageDO.setNextStageLables(JSON.toJSONString(stage.getNextStageLabels()));
                stageDOList.add(stageDO);
            }
            this.jobStages = stageDOList;
        }
    }

    public void createStageRelation(List<ChainPipeline<?>> pipelines) {
        if (pipelines == null) {
            return;
        }
        List<StageRelation> stageRelations = new ArrayList<>();
        for (ChainPipeline chainPipeline : pipelines) {
            List<StageGroup> groups = chainPipeline.getRootStageGroups();
            for (StageGroup group : groups) {
                StageRelation stageRelationDO = buildStageRelation(group, getName());
                stageRelationDO.setParentName(null);
                stageRelations.add(stageRelationDO);
                deepBuild(group.getChildren(), stageRelations, getName());
            }

        }
        this.stageRelations = stageRelations;

    }

    protected void createJobConfiguables(List<IConfigurable> configurables) {
        if (configurables == null) {
            return;
        }
        List<JobConfigure> jobConfigures = new ArrayList<>();
        for (IConfigurable configurable : configurables) {
            JobConfigure jobConfigure = new JobConfigure();
            jobConfigure.setJobName(getName());
            jobConfigure.setName(configurable.getName());
            jobConfigure.setConfigureNamespace(configurable.getNameSpace());
            jobConfigure.setConfigureType(configurable.getType());
            jobConfigures.add(jobConfigure);
        }
        JobConfigure relationDO = new JobConfigure();
        relationDO.setJobName(getName());
        relationDO.setName(getName());
        relationDO.setConfigureNamespace(DEFAULT_NAMESPACE);
        relationDO.setConfigureType("stream_task");
        jobConfigures.add(relationDO);
        this.jobConfigures = jobConfigures;
    }

    public List<JobConfigure> getJobConfigures() {
        return jobConfigures;
    }

    public void setJobConfigures(List<JobConfigure> jobConfigures) {
        this.jobConfigures = jobConfigures;
    }

    public List<StageRelation> getStageRelations() {
        return stageRelations;
    }

    public void setStageRelations(List<StageRelation> stageRelations) {
        this.stageRelations = stageRelations;
    }

    public List<JobStage> getJobStages() {
        return jobStages;
    }

    public void setJobStages(List<JobStage> jobStages) {
        this.jobStages = jobStages;
    }

    public List<String> getPipelineNamesBySource() {
        return pipelineNamesBySource;
    }

    public void setPipelineNamesBySource(List<String> pipelineNamesBySource) {
        this.pipelineNamesBySource = pipelineNamesBySource;
    }

    public void setPipelines(List<ChainPipeline<?>> pipelines) {
        if (pipelines == null) {
            return;
        }
        this.pipelinesBySource = pipelines;
        List<String> pipelineNames = new ArrayList<>();
        for (ChainPipeline chainPipeline : pipelines) {
            pipelineNames.add(chainPipeline.getName());
        }
        setPipelineNamesBySource(pipelineNames);
    }
}
