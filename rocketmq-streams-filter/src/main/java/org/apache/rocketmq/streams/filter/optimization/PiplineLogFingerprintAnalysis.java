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
package org.apache.rocketmq.streams.filter.optimization;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.metadata.MetaDataField;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.topology.stages.ScriptChainStage;
import org.apache.rocketmq.streams.common.topology.stages.UnionChainStage;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.filter.builder.ExpressionBuilder;
import org.apache.rocketmq.streams.script.utils.FunctionUtils;

/**
 * 通过分析找到pipline的字段指纹，分析的逻辑是以前置的rule作为起点，用到的字段追溯最原始表的字段 优化逻辑：在filterstage如果过滤失败，则设置指纹，下次同指纹的日志，不会走pipline处理 优化条件：尽可能找到过滤率高，且字段增加少的，优化顺序，先找第一个filter，如果必要可以再往后找filter，只要遇到不合适的stage（join/统计/或不友好的指纹字段等），则优化停止
 */
public class PiplineLogFingerprintAnalysis {
    protected FilterChainStage logFingerprintFliterChainStage;//最后优化到哪个filter
    protected ChainPipeline pipline;//待优化的pipline
    protected int filterIndex = 1;//优化到第几个filter就结束，-1表示优化到不能优化。

    public PiplineLogFingerprintAnalysis(ChainPipeline pipline) {
        this.pipline = pipline;
    }
    public PiplineLogFingerprintAnalysis(ChainPipeline pipline,int filterIndex) {
        this.pipline = pipline;
        this.filterIndex=filterIndex;
    }
    /**
     * 通过分析pipline找到可以过滤的日志指纹，插入到合适的stage中。如最顶部的stage中
     *
     * @return
     */
    public List<String> analysisPipline() {
        /**
         * 日志指纹，通过寻找fiter用到的字段获取日志指纹
         */
        Set<String> logFingerFieldNames = new HashSet<>();
        String stageName = null;
        if (pipline.isTopology()) {
            List<String> nextLables = pipline.getChannelNextStageLabel();
            for (String label : nextLables) {
                Map<String, AbstractStage> stageMap = pipline.getStageMap();
                AbstractStage rootStage = stageMap.get(label);
                //处理一个分支，目前只支持最顶层的分支，后续再有分支暂不支持
                AbstractStage stage = analysisStage(rootStage, logFingerFieldNames, null, 0);
                logFingerprintFliterChainStage = (FilterChainStage)stage;
                stageName = stage.getLabel();

            }
        } else {
            List<AbstractStage> stages = pipline.getStages();
            AbstractStage rootStage = stages.get(0);
            AbstractStage stage = analysisStage(rootStage, logFingerFieldNames, null, 0);
            logFingerprintFliterChainStage = (FilterChainStage)getPrewScriptChainStage(stage, FilterChainStage.class);
            int i = 0;
            for (AbstractStage stage1 : stages) {
                if (stage == stage1) {
                    break;
                }
                i++;
            }
            stageName = i + "";
        }

        if (logFingerFieldNames == null || logFingerFieldNames.size() == 0) {
            return null;
        }
        List<String> logFingerList = new ArrayList<>();
        logFingerList.addAll(logFingerFieldNames);
        Collections.sort(logFingerList);
        String key = MapKeyUtil.createKeyBySign(".", pipline.getNameSpace(), pipline.getConfigureName(), stageName);
        String value = MapKeyUtil.createKeyFromCollection(",", logFingerList);
        System.out.println(key + "=" + value);
        return logFingerList;
    }

    /**
     * 遍历所有节点，对可以优化的节点进行优化，在遇到第一个不能优化的节点就返回
     *
     * @param currentStage
     * @param logFingerFieldNames
     * @return
     */
    private AbstractStage analysisStage(AbstractStage currentStage, Set<String> logFingerFieldNames,
                                        FilterChainStage prewFilterStage, int filterStageIndex) {
        ChainPipeline pipline = (ChainPipeline)currentStage.getPipeline();
        if (currentStage == null) {
            return prewFilterStage;
        }
        if (pipline.isTopology() && currentStage.getNextStageLabels().size() > 1) {
            return prewFilterStage;
        }
        if (currentStage.isAsyncNode()) {
            return prewFilterStage;
        }
        if (FilterChainStage.class.isInstance(currentStage)) {
            FilterChainStage filterChainStage = (FilterChainStage)currentStage;
            if (filterStageIndex >= filterIndex) {
                return prewFilterStage;
            }
            /**
             * 获取日志指纹
             */
            Set<String> filterLogFingerprints = fetchLogFingerprint(filterChainStage);
            if (filterLogFingerprints == null) {
                return prewFilterStage;
            }

            logFingerFieldNames.addAll(filterLogFingerprints);
            prewFilterStage = (FilterChainStage)currentStage;
            filterStageIndex++;

        } else if (ScriptChainStage.class.isInstance(currentStage)) {
            //continue
        } else if (UnionChainStage.class.isInstance(currentStage)) {
            return prewFilterStage;

        } else {
            return prewFilterStage;
        }
        AbstractStage nextStage = getNextStage(currentStage);

        AbstractStage stage = analysisStage(nextStage, logFingerFieldNames, prewFilterStage, filterStageIndex);
        return stage;
    }

    /**
     * 如果包含过滤不友好的函数，可以直接结束优化 如过滤函数很少，且是非等值比较
     *
     * @param rules
     * @return
     */
    protected boolean hasExcludeFunctionNames(AbstractRule[] rules) {
        return false;

    }

    /**
     * 找到filter节点，做优化处理 如果遇到window节点，优化停止 如果遇到分叉节点，优化停止 主要针对script，fliter和union做有优化，遇到其他stage，优化停止
     *
     * @param filterChainStage
     * @return
     */
    private Set<String> fetchLogFingerprint(FilterChainStage filterChainStage) {
        ChainPipeline pipline = (ChainPipeline)filterChainStage.getPipeline();
        List<AbstractRule> rules=filterChainStage.getRules();
        if (rules == null) {
            return null;
        }
        /**
         * 规则依赖的所有字段
         */
        Set<String> dependentFields = new HashSet<>();
        for (AbstractRule rule : rules) {
            Set<String> set = rule.getDependentFields();
            dependentFields.addAll(set);
        }
        Set<String> logFingerFieldNames = new HashSet<>();
        /**
         * 获取最近的script stage
         */
        ScriptChainStage scriptChainStage = (ScriptChainStage)getPrewScriptChainStage(filterChainStage, ScriptChainStage.class);
        for (String field : dependentFields) {
            Set<String> metaDataFields = getChannelMetaField(field, pipline.getChannelMetaData(), scriptChainStage);
            if (metaDataFields == null) {
                return null;
            }
            logFingerFieldNames.addAll(metaDataFields);
        }
        return logFingerFieldNames;
    }

    /**
     * 找到当前字段依赖哪几个数据源字段
     *
     * @param fieldName 当前字段
     * @param metaData  数据源的字段描述
     * @return 依赖的数据源字段。如果不能找到，则返回null
     */
    public Set<String> getChannelMetaField(String fieldName, MetaData metaData, ScriptChainStage scriptChainStage) {
        Set<String> channelMetaFields = new HashSet<>();
        /**
         * 如果字段就是数据源字段，直接返回
         */
        String metaDataFieldName = getMetaDataFieldName(metaData, fieldName);
        if (metaDataFieldName != null) {

            channelMetaFields.add(metaDataFieldName);
            return channelMetaFields;
        }
        /**
         * 如果没有脚本返回null
         */
        if (scriptChainStage.getScript() == null) {
            return null;
        }
        /**
         * 处理过程中，如果已经处理过这个依赖，则不再处理，避免产生环
         */
        Set<String> existDependentFields = new HashSet<>();
        /**
         * 找到script中新产生的字段和依赖字段的关联关系
         */
        Map<String, List<String>> scriptDependentFields = scriptChainStage.getScript().getDependentFields();
        List<String> dependentFieldNames = scriptDependentFields.get(fieldName);
        Set<String> middleFieldNames = new HashSet<>();
        /**
         * 如果是原始字段直接返回，如果是非原始字段继续向上递归寻找
         */
        if (dependentFieldNames != null) {
            for (String name : dependentFieldNames) {
                name = FunctionUtils.getConstant(name);
                /**
                 * 如果字段是表达式，则先解析成rule，再找依赖
                 */
                if (name.startsWith("(") && name.endsWith(")")) {
                    AbstractRule rule = ExpressionBuilder.createRule("tmp", "tmp", name);
                    Set<String> ruleFieldNames = rule.getDependentFields();
                    for (String expressionName : ruleFieldNames) {
                        MetaDataField field = metaData.getMetaDataField(expressionName);
                        if (field != null) {
                            channelMetaFields.add(field.getFieldName());
                        } else {
                            /**
                             * 表达式的字段，在当前脚本中找当前字段的依赖字段，如果当前字段或依赖字段是数据源字段，放入channelMetaFields中，返回非数据源字段
                             */
                            Set<String> set = findDepedentFieldsFromSript(metaData, expressionName, scriptDependentFields, channelMetaFields, existDependentFields);
                            middleFieldNames.addAll(set);
                        }
                    }
                    continue;
                }
                MetaDataField field = metaData.getMetaDataField(name);
                if (field != null) {
                    channelMetaFields.add(field.getFieldName());
                } else {
                    Set<String> set = findDepedentFieldsFromSript(metaData, name, scriptDependentFields, channelMetaFields, existDependentFields);
                    middleFieldNames.addAll(set);
                }
            }
        } else {
            middleFieldNames.add(fieldName);
        }
        if (middleFieldNames.size() == 0) {
            return channelMetaFields;
        }
        ScriptChainStage prewScriptChainStage = (ScriptChainStage)getPrewScriptChainStage(scriptChainStage, ScriptChainStage.class);
        if (prewScriptChainStage == null) {
            return null;
        }
        for (String middleFieldName : middleFieldNames) {
            Set<String> dependentMetaFields = getChannelMetaField(middleFieldName, metaData, prewScriptChainStage);
            if (dependentMetaFields != null) {
                channelMetaFields.addAll(dependentMetaFields);
            } else {
                return null;
            }
        }
        return channelMetaFields;
    }

    /**
     * 找到这个script的上一个script stage
     *
     * @param currentStage
     * @return
     */
    protected AbstractStage getPrewScriptChainStage(AbstractStage currentStage, Class stageClass) {
        ChainPipeline pipline = (ChainPipeline)currentStage.getPipeline();
        /**
         * 如果是拓扑，则通过拓扑寻找，只支持单链模式。如果有两个来源，则不支持
         */
        if (pipline.isTopology()) {
            List<String> lables = currentStage.getPrevStageLabels();
            if (lables == null || lables.size() != 1) {
                return null;
            }
            Map<String, AbstractStage> stageMap = pipline.getStageMap();
            AbstractStage prewStage = stageMap.get(lables.get(0));
            if (stageClass.isInstance(prewStage)) {
                return prewStage;
            }
            if (prewStage == null) {
                return null;
            }
            return getPrewScriptChainStage(prewStage, stageClass);
        } else {
            /**
             * 如果是简单模式，通过list寻找
             */
            List<AbstractStage> stages = pipline.getStages();
            AbstractStage prewStage = null;
            for (AbstractStage stage : stages) {
                if (stage == currentStage) {
                    return prewStage;
                }
                if (stageClass.isInstance(stage)) {
                    prewStage = stage;
                }
            }
            return null;
        }
    }

    /**
     * 在当前脚本中找当前字段的依赖字段，如果当前字段或依赖字段是数据源字段，放入channelMetaFields中，返回非数据源字段 从这个脚本产生的新变量中，寻找metadata field
     *
     * @param metaData           原始字段
     * @param middleFieldName    待查找的字段
     * @param dependentFields    script产生的所有字段和依赖字段的关系
     * @param metaDataFieldNames 找到的字段放在这个list中
     * @return 返回没有找到字段
     */
    protected Set<String> findDepedentFieldsFromSript(MetaData metaData, String middleFieldName, Map<String, List<String>> dependentFields, Set<String> metaDataFieldNames, Set<String> existDependentFields) {
        Set<String> failMiddleFieldNames = new HashSet<>();
        middleFieldName = FunctionUtils.getConstant(middleFieldName);
        if (middleFieldName.startsWith("(") && middleFieldName.endsWith(")")) {
            AbstractRule rule = ExpressionBuilder.createRule("tmp", "tmp", middleFieldName);
            Set<String> expressionVars = rule.getDependentFields();
            for (String expressionVar : expressionVars) {
                MetaDataField field = metaData.getMetaDataField(expressionVar);
                if (field != null) {
                    metaDataFieldNames.add(field.getFieldName());
                } else {
                    Set<String> tmp = findDepedentFieldsFromSript(metaData, expressionVar, dependentFields, metaDataFieldNames, existDependentFields);
                    failMiddleFieldNames.addAll(tmp);
                }
            }
            return failMiddleFieldNames;
        }
        if (existDependentFields.contains(middleFieldName)) {
            failMiddleFieldNames.add(middleFieldName);
            return existDependentFields;
        } else {
            existDependentFields.add(middleFieldName);
        }
        List<String> dependentFieldNames = dependentFields.get(middleFieldName);
        if (dependentFieldNames == null || dependentFieldNames.size() == 0) {
            failMiddleFieldNames.add(middleFieldName);
            return failMiddleFieldNames;
        }
        for (String dependentFieldName : dependentFieldNames) {
            String fieldName = getMetaDataFieldName(metaData, dependentFieldName);
            if (fieldName != null) {
                metaDataFieldNames.add(fieldName);
            } else {
                Set<String> tmp = findDepedentFieldsFromSript(metaData, dependentFieldName, dependentFields, metaDataFieldNames, existDependentFields);
                if (tmp != null) {
                    failMiddleFieldNames.addAll(tmp);
                }
            }
        }
        return failMiddleFieldNames;
    }

    protected String getMetaDataFieldName(MetaData metaData, String fieldName) {
        MetaDataField metaDataField = metaData.getMetaDataField(fieldName);
        if (metaDataField == null) {
            return null;
        }
        return metaDataField.getFieldName();
    }

    /**
     * 获取当前节点的下一个节点
     *
     * @param currentStage
     * @return
     */
    protected AbstractStage getNextStage(AbstractStage currentStage) {
        ChainPipeline pipline = (ChainPipeline)currentStage.getPipeline();
        if (pipline.isTopology()) {
            List<String> nextLables = currentStage.getNextStageLabels();
            if (nextLables == null || nextLables.size() != 1) {
                return null;
            }
            Map<String, AbstractStage> stageMap = pipline.getStageMap();
            return stageMap.get(nextLables.get(0));
        }
        List<AbstractStage> stages = pipline.getStages();
        int i = 0;
        for (; i < stages.size(); i++) {
            if (stages.get(i) == currentStage) {
                break;
            }
        }
        i = i + 1;
        if (i > stages.size() - 1) {
            return null;
        }
        return stages.get(i);

    }

    public String createLogFingerprint(ChainPipeline pipline) {
        List<String> logFingerList = analysisPipline();
        return pipline.getNameSpace() + ">>" + pipline.getConfigureName() + "=" + MapKeyUtil.createKey(",", logFingerList);
    }

    public int getFilterIndex() {
        return filterIndex;
    }

    public void setFilterIndex(int filterIndex) {
        this.filterIndex = filterIndex;
    }

    public FilterChainStage getLogFingerprintFliterChainStage() {
        return logFingerprintFliterChainStage;
    }
}
