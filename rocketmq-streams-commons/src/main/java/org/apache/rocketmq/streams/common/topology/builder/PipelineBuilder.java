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
import java.util.Properties;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.model.NameCreatorContext;
import org.apache.rocketmq.streams.common.topology.IStageBuilder;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.utils.ENVUtil;
import org.apache.rocketmq.streams.common.utils.ReflectUtil;
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
     * 配置信息
     */
    protected Properties configuration;

    /**
     * 数据源的格式，非必须
     */

    protected MetaData channelMetaData;

    /**
     * 如果需要制作拓扑结构，则保存当前构建的stage
     */
    protected AbstractChainStage<?> currentChainStage;

    /**
     * 在sql tree中，存储当前节点父节点的table name，主要用于双流join场景，用于判断是否是右流join
     */
    protected String parentTableName;

    /**
     * 主要用在双流join的右流，是否是右流
     */

    protected boolean isRightJoin = false;

    /**
     * SQL Tree保存的是root table name
     */
    protected String rootTableName;

    /**
     * 主要用于把sql 按create view聚合，然后分层展开，便于监控和排错，主要用于sql解析时，完成stage的分组和层次关系
     */
    protected StageGroup currentStageGroup;

    protected StageGroup parentStageGroup;

    public PipelineBuilder(String namespace, String pipelineName, Properties properties) {
        this.pipelineNameSpace = namespace;
        this.pipelineName = pipelineName;
        this.configuration = properties;
        pipeline.setNameSpace(namespace);
        pipeline.setName(pipelineName);
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
     */
    public AbstractChainStage<?> createStage(IStageBuilder<AbstractChainStage<?>> stageBuilder) {
        AbstractChainStage<?> chainStage = stageBuilder.createStageChain(this);
        // 这句一定要在addChainStage前，会默认赋值namespace和name
        stageBuilder.addConfigurables(this);
        if (StringUtil.isEmpty(chainStage.getLabel())) {
            chainStage.setLabel(createConfigurableName(chainStage.getType()));
        }
        this.pipeline.addChainStage(chainStage);
        return chainStage;
    }

    public ChainPipeline<?> build() {
        pipeline.setChannelMetaData(channelMetaData);
        if (configurables != null) {
            for (IConfigurable configurable : configurables) {
                configurable.setJobName(pipelineName);
                configurable.setConfiguration(this.configuration);
                initAndENVReplace(configurable, this.configuration);
            }

        }
        return pipeline;
    }

    protected void initAndENVReplace(IConfigurable configurable, Properties configuration) {
        try {
            doEVNReplace(configurable, configuration);
        } catch (Exception e) {
            throw new RuntimeException("the configurable do evn replace error " + configurable.getClass().getName());
        }
        try {
            configurable.init();
        } catch (Exception e) {
            throw new RuntimeException("the configurable do init error " + configurable.getClass().getName());
        }

    }

    protected void doEVNReplace(IConfigurable configurable, Properties configuration) {
        ReflectUtil.scanConfiguableFields(configurable, (o, field) -> {
            ENVDependence dependence = field.getAnnotation(ENVDependence.class);
            if (dependence == null) {
                return;
            }
            try {
                field.setAccessible(true);
                if (!field.getType().getName().equals(String.class.getName())) {
                    return;
                }
                String fieldValue = ReflectUtil.getBeanFieldValue(o, field.getName());
                String envValue = getENVVar(fieldValue, configuration);
                if (StringUtil.isEmpty(envValue)) {
                    return;
                }
                ReflectUtil.setBeanFieldValue(o, field.getName(), envValue);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }

        });
    }

    protected String getENVVar(String fieldValue, Properties configuration) {
        if (StringUtil.isEmpty(fieldValue)) {
            return null;
        }
        String value = configuration.getProperty(fieldValue);
        if (StringUtil.isNotEmpty(value)) {
            return value;
        }
        return ENVUtil.getENVParameter(fieldValue);
    }

    public List<IConfigurable> getAllConfigurables() {
        return configurables;
    }

    public void addChainStage(AbstractChainStage<?> chainStage) {
        if (StringUtil.isEmpty(chainStage.getName())) {
            chainStage.setName(createConfigurableName(chainStage.getType()));
            chainStage.setLabel(chainStage.getName());
        }
        pipeline.addChainStage(chainStage);
    }

    /**
     * 创建chain stage
     */
    public AbstractChainStage<?> createStage(ISink<?> sink) {
        OutputChainStage<?> outputChainStage = new OutputChainStage();
        sink.addConfigurables(this);
        outputChainStage.setSink(sink);
        if (StringUtil.isEmpty(sink.getName())) {
            sink.setName(createConfigurableName(sink.getType()));
        }
        pipeline.addChainStage(outputChainStage);
        return outputChainStage;
    }

    /**
     * 增加输出
     */
    public OutputChainStage<?> addOutput(ISink<?> sink) {
        OutputChainStage<?> outputChainStage = new OutputChainStage<>();
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
     */
    public void addNameList(IConfigurable configurable) {
        addConfigurables(configurable);
    }

    /**
     * 增加中间chain stage
     */
    public AbstractChainStage<?> addChainStage(IStageBuilder<AbstractChainStage<?>> stageChainBuilder) {
        return createStage(stageChainBuilder);
    }

    /**
     * 自动创建组建名称
     */
    public String createConfigurableName(String type) {
        return NameCreatorContext.get().createName(this.pipelineName, type);
    }

    /**
     * 保存中间产生的结果
     */
    public void addConfigurables(IConfigurable configurable) {
        if (configurable != null) {
            if (StringUtil.isEmpty(configurable.getNameSpace())) {
                configurable.setNameSpace(getPipelineNameSpace());
            }
            if (StringUtil.isEmpty(configurable.getName())) {
                configurable.setName(createConfigurableName(configurable.getType()));
            }
            //判断配置信息是否已经存在，如果不存在，则添加
            for (IConfigurable config : this.configurables) {
                if (config.getType().equals(configurable.getType()) && config.getName().equals(configurable.getName())) {
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
     */
    public void setTopologyStages(AbstractChainStage<?> currentChainStage, List<AbstractChainStage<?>> nextStages) {
        if (nextStages == null) {
            return;
        }
        List<String> labelNames = new ArrayList<>();
        for (AbstractChainStage<?> stage : nextStages) {
            labelNames.add(stage.getLabel());
        }

        if (currentChainStage == null) {
            this.pipeline.setChannelNextStageLabel(labelNames);
        } else {
            currentChainStage.getNextStageLabels().addAll(labelNames);
            for (AbstractChainStage<?> stage : nextStages) {
                stage.getPrevStageLabels().add(currentChainStage.getLabel());
            }
        }
    }

    /**
     * 拓扑的特殊形式，下层只有单个节点
     */
    public void setTopologyStages(AbstractChainStage<?> currentChainStage, AbstractChainStage<?> nextStage) {
        List<AbstractChainStage<?>> stages = new ArrayList<>();
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

    public void setHorizontalStages(AbstractChainStage<?> stage) {
        List<AbstractChainStage<?>> stages = new ArrayList<>();
        stages.add(stage);
        setHorizontalStages(stages);
    }

    /**
     * 如果需要做拓扑，需要设置标签
     */
    public void setHorizontalStages(List<AbstractChainStage<?>> stages) {
        if (stages == null) {
            return;
        }
        List<String> labelNames = new ArrayList<>();
        Map<String, AbstractChainStage<?>> labelName2Stage = new HashMap();
        for (AbstractChainStage<?> stage : stages) {
            if (stage == null) {
                continue;
            }
            labelNames.add(stage.getLabel());
            labelName2Stage.put(stage.getLabel(), stage);
        }

        if (currentChainStage == null) {
            this.pipeline.setChannelNextStageLabel(labelNames);
            for (String lableName : labelNames) {
                AbstractChainStage<?> chainStage = labelName2Stage.get(lableName);
                if (this.pipeline.getChannelName() != null) {
                    chainStage.getPrevStageLabels().add(this.pipeline.getChannelName());
                }
            }
        } else {
            if (currentChainStage.getNextStageLabels() == null) {
                currentChainStage.setNextStageLabels(new ArrayList<>());
            }
            for (String labelName : labelNames) {
                if (StringUtil.isEmpty(labelName)) {
                    continue;
                }
                if (!currentChainStage.getNextStageLabels().contains(labelName)) {
                    currentChainStage.getNextStageLabels().add(labelName);
                }
                AbstractChainStage<?> chainStage = labelName2Stage.get(labelName);
                List<String> prevLabels = chainStage.getPrevStageLabels();
                if (!prevLabels.contains(this.currentChainStage.getLabel())) {
                    chainStage.getPrevStageLabels().add(this.currentChainStage.getLabel());
                }

            }
        }
    }

    public List<AbstractChainStage<?>> getFirstStages() {
        Map<String, AbstractStage<?>> stageMap = pipeline.createStageMap();
        List<AbstractChainStage<?>> stages = new ArrayList<>();
        List<String> labels = pipeline.getChannelNextStageLabel();
        if (labels == null) {
            return null;
        }
        for (String labelName : labels) {
            stages.add((AbstractChainStage) stageMap.get(labelName));
        }
        return stages;
    }

    public AbstractChainStage<?> getCurrentChainStage() {
        return currentChainStage;
    }

    public void setCurrentChainStage(AbstractChainStage<?> currentChainStage) {
        this.currentChainStage = currentChainStage;
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

    public Properties getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Properties configuration) {
        this.configuration = configuration;
    }
}
