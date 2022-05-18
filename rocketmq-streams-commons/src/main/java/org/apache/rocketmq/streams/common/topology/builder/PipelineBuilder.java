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
package org.apache.rocketmq.streams.common.topology.builder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.channel.impl.view.ViewSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configurable.AbstractConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.NameCreator;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ViewChainStage;
import org.apache.rocketmq.streams.common.utils.StringUtil;

public class PipelineBuilder implements Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * 最终产出的pipeline
     */
    protected ChainPipeline<?> pipeline = new ChainPipeline<>();

    /**
     * 保存pipeline构建过程中产生的configurable对象
     */
    protected List<IConfigurable> configurables = new ArrayList<>();

    /**
     * pipeline namespace
     */
    protected String pipelineNameSpace;

    /**
     * pipeline name
     */
    protected String pipelineName;

    /**
     * 数据源的格式，非必须
     */

    protected MetaData channelMetaData;

    /**
     * 如果需要制作拓扑结构，则保存当前构建的stage
     */
    protected ChainStage<?> currentChainStage;

    /**
     * 在sql tree中，存储当前节点父节点的table name，主要用于双流join场景，用于判断是否是右流join
     */
    protected String parentTableName;

    /**
     * 主要用在双流join的右流，是否是右流
     */

    protected boolean isRightJoin = false;


    protected String rootTableName;//SQL Tree保存的是root tablename

    /**
     * 主要用于把sql 按create view聚合，然后分层展开，便于监控和排错，主要用于sql解析时，完成stage的分组和层次关系
     */
    protected StageGroup currentStageGroup;

    protected StageGroup parentStageGroup;

    public PipelineBuilder(String namespace, String pipelineName) {
        pipeline.setNameSpace(namespace);
        pipeline.setConfigureName(pipelineName);
        this.pipelineNameSpace = namespace;
        this.pipelineName = pipelineName;
        addConfigurables(pipeline);
    }

    /**
     * 设置pipeline的source
     *
     * @param source 数据源
     */
    public void setSource(ISource<?> source) {
        source.createStageChain(this);
        source.addConfigurables(this);
        this.pipeline.setSource(source);
    }

    /**
     * 创建chain stage
     *
     * @param stageBuilder
     * @return
     */
    public ChainStage<?> createStage(IStageBuilder<ChainStage> stageBuilder) {
        ChainStage<?> chainStage = stageBuilder.createStageChain(this);
        stageBuilder.addConfigurables(this);// 这句一定要在addChainStage前，会默认赋值namespace和name
        if (StringUtil.isEmpty(chainStage.getLabel())) {
            chainStage.setLabel(createConfigurableName(chainStage.getType()));
        }
        this.pipeline.addChainStage(chainStage);
        return chainStage;
    }

    public List<String> createSQL() {
        List<String> sqls = new ArrayList<>();
        for (IConfigurable configurable : configurables) {
            sqls.add(AbstractConfigurable.createSQL(configurable));
        }
        return sqls;
    }

    public ChainPipeline<?> build(IConfigurableService configurableService) {
        List<IConfigurable> configurableList = configurables;
        pipeline.setChannelMetaData(channelMetaData);
        if (configurableList != null) {
            for (IConfigurable configurable : configurableList) {
                configurableService.insert(configurable);
            }
        }
        configurableService.refreshConfigurable(pipelineNameSpace);
        return configurableService.queryConfigurable(Pipeline.TYPE, pipelineName);
    }

    public List<IConfigurable> getAllConfigurables() {
        List<IConfigurable> configurableList = configurables;
        pipeline.setChannelMetaData(channelMetaData);
        return configurableList;
    }

    /**
     * 创建chain stage
     *
     * @param sink
     * @return
     */
    public ChainStage<?> createStage(ISink<?> sink) {
        OutputChainStage<?> outputChainStage = new OutputChainStage();
        sink.addConfigurables(this);
        outputChainStage.setSink(sink);
        if (StringUtil.isEmpty(sink.getConfigureName())) {
            sink.setConfigureName(createConfigurableName(sink.getType()));
        }
        pipeline.addChainStage(outputChainStage);
        return outputChainStage;
    }

    /**
     * 增加输出
     *
     * @param sink
     * @return
     */
    public OutputChainStage<?> addOutput(ISink<?> sink) {
        OutputChainStage<?> outputChainStage = new OutputChainStage<>();;
        if(ViewSink.class.isInstance(sink)){

            outputChainStage=new ViewChainStage();
        }
        sink.addConfigurables(this);
        outputChainStage.setSink(sink);
        if (StringUtil.isEmpty(outputChainStage.getLabel())) {
            outputChainStage.setLabel(createConfigurableName(outputChainStage.getType()));
        }
        pipeline.addChainStage(outputChainStage);
        return outputChainStage;
    }

    /**
     * 增加维表
     *
     * @param configurable
     */
    public void addNameList(IConfigurable configurable) {
        addConfigurables(configurable);
    }

    /**
     * 增加中间chain stage
     *
     * @param stageChainBuilder
     */
    public ChainStage<?> addChainStage(IStageBuilder<ChainStage> stageChainBuilder) {
       return createStage(stageChainBuilder);
    }

    /**
     * 自动创建组建名称
     *
     * @param type
     * @return
     */
    public String createConfigurableName(String type) {
        return NameCreatorContext.get().createNewName(this.pipelineName, type);
    }

    /**
     * 保存中间产生的结果
     */
    public void addConfigurables(IConfigurable configurable) {
        if (configurable != null) {
            if (StringUtil.isEmpty(configurable.getNameSpace())) {
                configurable.setNameSpace(getPipelineNameSpace());
            }
            if (StringUtil.isEmpty(configurable.getConfigureName())) {
                configurable.setConfigureName(createConfigurableName(configurable.getType()));
            }
            //判断配置信息是否已经存在，如果不存在，则添加
            for (IConfigurable config : this.configurables) {
                if (config.getType().equals(configurable.getType()) && config.getConfigureName().equals(configurable.getConfigureName())) {
                    return;
                }
            }
            this.configurables.add(configurable);
        }
    }

    public void addConfigurables(Collection<? extends IConfigurable> configurables) {

        if (configurables != null) {
            for (IConfigurable configurable : configurables) {
                addConfigurables(configurable);
            }
        }
    }

    /**
     * 在当前拓扑基础上，增加下一层级的拓扑。如果需要做拓扑，需要设置标签
     *
     * @param nextStages
     */
    public void setTopologyStages(ChainStage<?> currentChainStage, List<ChainStage> nextStages) {
        if (nextStages == null) {
            return;
        }
        List<String> labelNames = new ArrayList<>();
        for (ChainStage<?> stage : nextStages) {
            labelNames.add(stage.getLabel());
        }

        if (currentChainStage == null) {
            this.pipeline.setChannelNextStageLabel(labelNames);
        } else {
            currentChainStage.getNextStageLabels().addAll(labelNames);
            for (ChainStage<?> stage : nextStages) {
                stage.getPrevStageLabels().add(currentChainStage.getLabel());
            }
        }
    }

    /**
     * 拓扑的特殊形式，下层只有单个节点
     *
     * @param nextStage
     */
    public void setTopologyStages(ChainStage<?> currentChainStage, ChainStage<?> nextStage) {
        List<ChainStage> stages = new ArrayList<>();
        stages.add(nextStage);
        setTopologyStages(currentChainStage, stages);
    }

    public String getPipelineNameSpace() {
        return pipelineNameSpace;
    }

    public String getPipelineName() {
        return pipelineName;
    }

    public ChainPipeline<?> getPipeline() {
        return pipeline;
    }

    public void setHorizontalStages(ChainStage<?> stage) {
//        if (isBreak) {
//            return;
//        }
        List<ChainStage<?>> stages = new ArrayList<>();
        stages.add(stage);
        setHorizontalStages(stages);
    }

    /**
     * 如果需要做拓扑，需要设置标签
     *
     * @param stages
     */
    public void setHorizontalStages(List<ChainStage<?>> stages) {
        if (stages == null) {
            return;
        }
        List<String> lableNames = new ArrayList<>();
        Map<String, ChainStage<?>> lableName2Stage = new HashMap();
        for (ChainStage<?> stage : stages) {
            if(stage==null){
                continue;
            }
            lableNames.add(stage.getLabel());
            lableName2Stage.put(stage.getLabel(), stage);
        }

        if (currentChainStage == null) {
            this.pipeline.setChannelNextStageLabel(lableNames);
            for (String lableName : lableNames) {
                ChainStage<?> chainStage = lableName2Stage.get(lableName);
                if(this.pipeline.getChannelName()!=null){
                    chainStage.getPrevStageLabels().add(this.pipeline.getChannelName());
                }
            }
        } else {
            if(currentChainStage.getNextStageLabels()==null){
                currentChainStage.setNextStageLabels(new ArrayList<>());
            }
            for (String lableName : lableNames) {
                if(StringUtil.isEmpty(lableName)){
                    continue;
                }
                if(!currentChainStage.getNextStageLabels().contains(lableName)){
                    currentChainStage.getNextStageLabels().add(lableName);
                }
                ChainStage<?> chainStage = lableName2Stage.get(lableName);
                List<String> prewLables = chainStage.getPrevStageLabels();
                if (!prewLables.contains(this.currentChainStage.getLabel())) {
                    chainStage.getPrevStageLabels().add(this.currentChainStage.getLabel());
                }

            }
        }
    }

    public List<ChainStage<?>> getFirstStages(){
        Map<String, AbstractStage<?>> stageMap= pipeline.createStageMap();
        List<ChainStage<?>> stages=new ArrayList<>();
        List<String> firstLables=pipeline.getChannelNextStageLabel();
        if(firstLables==null){
            return null;
        }
        for(String lableName:firstLables){
            stages.add((ChainStage) stageMap.get(lableName));
        }
        return stages;
    }
    public void setCurrentChainStage(ChainStage<?> currentChainStage) {
        this.currentChainStage = currentChainStage;
    }

    public ChainStage<?> getCurrentChainStage() {
        return currentChainStage;
    }

    public List<IConfigurable> getConfigurables() {
        return configurables;
    }

    public MetaData getChannelMetaData() {
        return channelMetaData;
    }

    public void setChannelMetaData(MetaData channelMetaData) {
        this.channelMetaData = channelMetaData;
    }

    public String getParentTableName() {
        return parentTableName;
    }

    public void setParentTableName(String parentTableName) {
        this.parentTableName = parentTableName;
    }

    public boolean isRightJoin() {
        return isRightJoin;
    }

    public void setRightJoin(boolean rightJoin) {
        isRightJoin = rightJoin;
    }

    public String getRootTableName() {
        return rootTableName;
    }

    public void setRootTableName(String rootTableName) {
        this.rootTableName = rootTableName;
    }

    public StageGroup getCurrentStageGroup() {
        return currentStageGroup;
    }

    public void setCurrentStageGroup(StageGroup currentStageGroup) {
        this.currentStageGroup = currentStageGroup;
    }

    public StageGroup getParentStageGroup() {
        return parentStageGroup;
    }

    public void setParentStageGroup(StageGroup parentStageGroup) {
        this.parentStageGroup = parentStageGroup;
    }
}
