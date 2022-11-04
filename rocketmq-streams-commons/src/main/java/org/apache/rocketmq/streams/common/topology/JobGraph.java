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
package org.apache.rocketmq.streams.common.topology;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.model.JobConfigure;
import org.apache.rocketmq.streams.common.model.JobStage;
import org.apache.rocketmq.streams.common.model.StageRelation;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.AbstractScript;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.utils.PrintUtil;

public class JobGraph extends BasedConfigurable {
    public static final String DEFAULT_NAMESPACE = "dipper.private.blink.rules";
    public static String TYPE="JobGraph";
    protected String userName;
    protected List<String> pipelineNames;
    protected List<JobConfigure> jobConfigures;
    protected List<StageRelation> stageRelations;
    protected List<JobStage> jobStages;

    public JobGraph(){
        setType(TYPE);
    }


    public void createJobInfo(List<ChainPipeline<?>> pipelines, List<IConfigurable> configurables){
        createJobConfiguables(configurables);
        createJobStages(pipelines);
        createStageRelation(pipelines);
    }



    private void createJobStages(List<ChainPipeline<?>> pipelines) {
        if(pipelines==null){
            return;
        }
        for(ChainPipeline pipeline:pipelines){
            List<JobStage> stageDOList = new ArrayList<>();
            int i = 0;
            List<AbstractStage<?>> stages=pipeline.getStages();
            for (AbstractStage<?> stage : stages) {
                List<String> prevStageLabels = stage.getPrevStageLabels();
                if (CollectionUtils.isEmpty(prevStageLabels) || StringUtils.equals(prevStageLabels.get(0), getConfigureName())) {
                    //上游为null 虚拟一个source
                    String sqlContent = new JSONObject()
                        .fluentPut("sql", pipeline.getCreateTableSQL())
                        //  .fluentPut("pos", pipeline.getCreateTableSQLPos())
                        .toJSONString();
                    JobStage source = new JobStage();
                    source.setJobName(getConfigureName());
                    source.setStageName(getConfigureName() + "_source_" + i++);
                    source.setMachineName("");
                    source.setStageType(StageType.SOURCE.getType());
                    source.setStageContent("");
                    source.setSqlContent(sqlContent);
                    source.setPrevStageLables("[]");
                    source.setNextStageLables("[\"" + stage.getLabel() + "\"]");
                    stageDOList.add(source);
                }
                JobStage stageDO = new JobStage();
                stageDO.setJobName(getConfigureName());
                stageDO.setStageName(stage.getLabel());
                stageDO.setMachineName("");
                stageDO.setStageType(StageType.getTypeForStage(stage));
                stageDO.setStageContent(getStageContent(stage));
                stageDO.setSqlContent(getSqlContent(stage));
                stageDO.setPrevStageLables(JSON.toJSONString(prevStageLabels));
                stageDO.setNextStageLables(JSON.toJSONString(stage.getNextStageLabels()));
                stageDOList.add(stageDO);
            }
            this.jobStages=stageDOList;
        }
    }

    public void createStageRelation(List<ChainPipeline<?>> pipelines) {
        if(pipelines==null){
            return;
        }
        List<StageRelation> stageRelations=new ArrayList<>();
        for(ChainPipeline chainPipeline:pipelines){
            List<StageGroup> groups = chainPipeline.getRootStageGroups();
            for (StageGroup group : groups) {
                StageRelation stageRelationDO = buildStageRelation(group, getConfigureName());
                stageRelationDO.setParentName(null);
                stageRelations.add(stageRelationDO);
                deepBuild(group.getChildren(), stageRelations, getConfigureName());
            }

        }
        this.stageRelations=stageRelations;

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
        StageRelation stageRelation= new StageRelation();
        stageRelation.setJobName(jobName);
        stageRelation.setGroupName(stageGroup.getConfigureName());
        stageRelation.setSqlContent(sqlContent);
        stageRelation.setViewName(stageGroup.getViewName());
        stageRelation.setStageLabels(JSON.toJSONString(stageGroup.getAllStageLables()));
        stageRelation.setStartLabel(stageGroup.getStartLable());
        stageRelation.setEndLabel(stageGroup.getEndLable());
        stageRelation.setParentName(stageGroup.getParentName());
        stageRelation.setChildrenNames(JSON.toJSONString(stageGroup.getChildrenNames()));
        return stageRelation;
    }


    protected void createJobConfiguables(List<IConfigurable> configurables) {
        if(configurables==null){
            return;
        }
        List<JobConfigure> jobConfigures=new ArrayList<>();
        for (IConfigurable configurable : configurables) {
            JobConfigure jobConfigure = new JobConfigure();
            jobConfigure.setJobName(getConfigureName());
            jobConfigure.setConfigureName(configurable.getConfigureName());
            jobConfigure.setConfigureNamespace(configurable.getNameSpace());
            jobConfigure.setConfigureType(configurable.getType());
            jobConfigures.add(jobConfigure);
        }
        JobConfigure relationDO = new JobConfigure();
        relationDO.setJobName(getConfigureName());
        relationDO.setConfigureName(getConfigureName());
        relationDO.setConfigureNamespace(DEFAULT_NAMESPACE);
        relationDO.setConfigureType("stream_task");
        jobConfigures.add(relationDO);
        this.jobConfigures=jobConfigures;
    }



    private static String getSqlContent(AbstractStage stage){
        return new JSONObject()
            .fluentPut("sql", stage.getSql())
            //  .fluentPut("pos", stage.getPos())
            .toJSONString();
    }


    protected static String getStageContent(AbstractStage stage){
        String type = StageType.getTypeForStage(stage);
        StringBuilder sb = new StringBuilder();
        switch (type){
            case "filter":
                FilterChainStage filterChainStage = (FilterChainStage) stage;
                List<AbstractRule> rules = filterChainStage.getRules();
                if(rules != null){
                    for (AbstractRule rule:rules){
                        sb.append(rule.toString()+ PrintUtil.LINE);
                    }
                }
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

    public void setPipelineNames(List<String> pipelineNames) {
        this.pipelineNames = pipelineNames;
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

    public String getJobName() {
        return getConfigureName();
    }

    public void setJobName(String jobName) {
        setConfigureName(jobName);
    }

    public List<String> getPipelineNames() {
        return pipelineNames;
    }

    public void setPipelines(List<ChainPipeline<?>> pipelines){
        if(pipelines==null){
            return;
        }
        List<String> pipelineNames=new ArrayList<>();
        for(ChainPipeline chainPipeline:pipelines){
            pipelineNames.add(chainPipeline.getConfigureName());
        }
        setPipelineNames(pipelineNames);
    }
}
