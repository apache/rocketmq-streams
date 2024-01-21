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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStage;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.optimization.MessageTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.metric.NotFireReason;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.metric.StageMetric;
import org.apache.rocketmq.streams.common.topology.stages.FilterChainStage;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.JsonableUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractStage<T extends IMessage> extends BasedConfigurable implements IStage<T> {
    public static final String TYPE = "stage";
    private static final Logger LOG = LoggerFactory.getLogger(AbstractStage.class);
    protected String filterFieldNames;
    protected transient String name;

    /**
     * 是否关闭拆分模式，多条日志会合并在一起，字段名为splitDataFieldName
     */
    protected boolean closeSplitMode = false;

    protected String splitDataFieldName;

    protected transient AbstractPipeline<?> pipeline;

    /**
     * 设置路由label，当需要做路由选择时需要设置
     */
    protected String label;

    /**
     * 如果是拓扑结构，则设置next节点的label name
     */
    protected List<String> nextStageLabels = new ArrayList<>();

    /**
     * 上游对应的label列表
     */
    protected List<String> prevStageLabels = new ArrayList<>();

    /**
     * 消息来源于哪个上游节点，对应的上游是sql node的输出表名，如create view tablename的tablename
     */
    protected String msgSourceName;

    /**
     * 这个stage所属哪段sql，sql的输出表名。如create view tablename的tablename
     */
    protected String ownerSqlNodeTableName;

    /**
     * 主要用于排错，把stage按sql分组，可以快速定位问题
     */
    protected String sql;
    protected transient StageGroup stageGroup;
    /**
     * 前置指纹记录
     */
    protected transient PreFingerprint preFingerprint = null;

    //监控信息
    protected transient StageMetric stageMetric = new StageMetric();

    public AbstractStage() {
        setType(TYPE);
    }

    /**
     * 执行一个stage
     *
     * @param t
     * @param context
     */
    @Override
    public boolean executeStage(T t, AbstractContext context) {
        if (t.getHeader().isSystemMessage()) {
            ISystemMessage systemMessage = t.getSystemMessage();
            if (systemMessage instanceof CheckPointMessage) {
                checkpoint(t, context, (CheckPointMessage) systemMessage);
            } else if (systemMessage instanceof NewSplitMessage) {
                addNewSplit(t, context, (NewSplitMessage) systemMessage);
            } else if (systemMessage instanceof RemoveSplitMessage) {
                removeSplit(t, context, (RemoveSplitMessage) systemMessage);
            } else if (systemMessage instanceof BatchFinishMessage) {
                batchMessageFinish(t, context, (BatchFinishMessage) systemMessage);
            } else {
                if (systemMessage == null) {
                    return true;
                }
                throw new RuntimeException("can not support this system message " + systemMessage.getClass().getName());
            }
            if (isAsyncNode()) {
                context.breakExecute();
                return false;
            }
            return true;
        }
        context.resetIsContinue();
        if (context.isSplitModel() && !isCloseSplitMode()) {
            List<T> oldSplits = context.getSplitMessages();
            List<T> newSplits = new ArrayList<T>();
            int splitMessageOffset = 0;
            boolean isFinishTrace = MessageTrace.existFinishBranch(t);
            for (T subT : oldSplits) {
                context.closeSplitMode(subT);
                subT.getHeader().setMsgRouteFromLable(t.getHeader().getMsgRouteFromLable());
                subT.getHeader().addLayerOffset(splitMessageOffset);
                splitMessageOffset++;
                Object result = this.doMessage(t, context);
                boolean isContinue = (result != null && context.isContinue());
                if (!isContinue) {
                    context.removeSpliteMessage(subT);
                    context.cancelBreak();
                    continue;
                }
                //lastMsg=subT;
                if (context.isSplitModel()) {
                    newSplits.addAll(context.getSplitMessages());
                } else {
                    newSplits.add(subT);
                }
            }
            MessageTrace.setResult(t, isFinishTrace);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
            //if (needFlush) {
            //    flushStage(stage, lastMsg, context);
            //}
            context.setSplitMessages(newSplits);
            context.openSplitModel();

            if (newSplits == null || newSplits.size() == 0) {
                context.breakExecute();
                return false;
            }

        } else {
            if (isCloseSplitMode()) {
                if (StringUtil.isNotEmpty(getSplitDataFieldName())) {
                    List<T> msgs = context.getSplitMessages();
                    JSONArray jsonArray = createJsonArray(msgs);
                    t.getMessageBody().put(getSplitDataFieldName(), jsonArray);
                }
                context.closeSplitMode(t);
            }
            Boolean isFinishTrace = MessageTrace.existFinishBranch(t);
            Object result = this.doMessage(t, context);
            boolean isContinue = (result != null && context.isContinue());
            MessageTrace.setResult(t, isFinishTrace);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
            return isContinue;
        }
        return true;
    }

    @Override
    public T doMessage(T t, AbstractContext<T> context) {
        long startTime = stageMetric.startCalculate(t);
        T result = null;
        try {
            TraceUtil.debug(t.getHeader().getTraceId(), "AbstractStage", label, t.getMessageBody().toJSONString());
            result = handleMessage(t, context);
            long cost = stageMetric.endCalculate(startTime);
            if (cost > 3000) {
                LOG.warn("[{}][{}] Stage_Slow_On[{}]----[{}]", IdUtil.instanceId(), this.getPipeline().getName(), this.getSql(), cost);
            }
            if (!context.isContinue() || result == null) {
                NotFireReason notFireReason = null;
                if ("true".equals(getConfiguration().getProperty(ConfigurationKey.MONITOR_PIPELINE_HTML_SWITCH))) {
                    notFireReason = createNotReason(t, context);
                    if (notFireReason != null) {
                        stageMetric.filterCalculate(notFireReason);
                    }
                }
                if (LOG.isDebugEnabled()) {

                    if (notFireReason == null) {
                        notFireReason = createNotReason(t, context);
                    }
                    if (notFireReason != null) {
                        String info = JsonableUtil.formatJson(notFireReason.toJson()).replace("<p>", "\n").replace("<br>", "\r");
                        LOG.debug("[{}][{}] Filter_NotMatch_Reason[{}]----\n\r[{}]", IdUtil.instanceId(), this.getPipeline().getName(), this.getSql(), info);
                    }

                }

                return (T) context.breakExecute();
            }
            stageMetric.outCalculate();
            context.removeNotFireReason();
        } catch (Exception e) {
            LOG.error("[{}][{}] Stage_Error_On({})-errorMsg({})", IdUtil.instanceId(), this.getPipeline().getName(), this.getSql(), e.getMessage(), e);
            context.breakExecute();
        }

        return result;
    }

    protected abstract T handleMessage(T t, AbstractContext context);

    /**
     * 是否是异步节点，流程会在此节点终止，启动新线程开启后续流程
     *
     * @return
     */
    public abstract boolean isAsyncNode();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    String toJsonString() {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name", getName());
        return jsonObject.toJSONString();
    }

    public boolean isCloseSplitMode() {
        return closeSplitMode;
    }

    public void setCloseSplitMode(boolean closeSplitMode) {
        this.closeSplitMode = closeSplitMode;
    }

    public String getSplitDataFieldName() {
        return splitDataFieldName;
    }

    public void setSplitDataFieldName(String splitDataFieldName) {
        this.splitDataFieldName = splitDataFieldName;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public List<String> doRoute(T t) {
        String routeLabel = t.getHeader().getRouteLabels();
        String filterLabel = t.getHeader().getFilterLabels();
        t.getHeader().setRouteLabels(null);
        t.getHeader().setFilterLabels(null);
        if (StringUtil.isEmpty(routeLabel) && StringUtil.isEmpty(filterLabel)) {
            return this.nextStageLabels;
        }

        List<String> labels = new ArrayList<>(this.nextStageLabels);
        if (StringUtil.isNotEmpty(routeLabel)) {
            Set<String> routeLabelSet = t.getHeader().createRouteLabelSet(routeLabel);
            labels = new ArrayList<>();
            for (String tempLabel : this.nextStageLabels) {
                if (routeLabelSet.contains(tempLabel)) {
                    labels.add(tempLabel);
                }
            }
        }
        if (StringUtil.isNotEmpty(filterLabel)) {
            Set<String> routeFilterLabelSet = t.getHeader().createRouteLabelSet(filterLabel);
            for (String tempLabel : this.nextStageLabels) {
                if (routeFilterLabelSet.contains(label)) {
                    labels.remove(tempLabel);
                }
            }
        }
        return labels;
    }

    /**
     * 从配置文件加载日志指纹信息，如果存在做指纹优化
     */
    protected PreFingerprint loadLogFinger() {
        ChainPipeline<?> pipeline = (ChainPipeline<?>) getPipeline();
        String filterName = getLabel();
        if (!pipeline.isTopology()) {
            List<AbstractStage<?>> stages = pipeline.getStages();
            int i = 0;
            for (AbstractStage<?> stage : stages) {
                if (stage == this) {
                    break;
                }
                i++;
            }
            filterName = i + "";
        }
        String stageIdentification = MapKeyUtil.createKeyBySign(".", pipeline.getNameSpace(), pipeline.getName(), filterName);
        if (this.filterFieldNames == null) {
            this.filterFieldNames = getConfiguration().getProperty(stageIdentification);
        }
        if (this.filterFieldNames == null) {
            return null;
        }
        PreFingerprint preFingerprint = createPreFingerprint(stageIdentification);
        if (preFingerprint != null) {
            pipeline.registPreFingerprint(preFingerprint);
        }
        return preFingerprint;
    }

    /**
     * 发现最源头的stage
     *
     * @return
     */

    protected PreFingerprint createPreFingerprint(String stageIdentification) {
        ChainPipeline<?> pipeline = (ChainPipeline<?>) getPipeline();
        String sourceLabel = null;
        String nextLabel = null;
        if (pipeline.isTopology()) {
            Map<String, AbstractStage<?>> stageMap = pipeline.createStageMap();
            AbstractStage<?> currentStage = this;
            List<String> preLabels = currentStage.getPrevStageLabels();
            while (preLabels != null && preLabels.size() > 0) {
                if (preLabels.size() > 1) {//union
                    sourceLabel = null;
                    nextLabel = null;
                    break;
                }
                String lable = preLabels.get(0);
                AbstractStage<?> stage = stageMap.get(lable);

                if (stage != null) {
                    if (stage.isAsyncNode()) {//window (join,Statistics)
                        sourceLabel = null;
                        nextLabel = null;
                        break;
                    }
                    nextLabel = currentStage.getLabel();
                    currentStage = stage;
                    sourceLabel = currentStage.getLabel();
                    if (stage.getNextStageLabels() != null && stage.getNextStageLabels().size() > 1) {
                        break;
                    }
                } else {
                    sourceLabel = pipeline.getChannelName();
                    nextLabel = currentStage.getLabel();
                    break;
                }
                preLabels = currentStage.getPrevStageLabels();
            }
            if (preLabels == null || preLabels.size() == 0) {
                sourceLabel = pipeline.getChannelName();
                nextLabel = currentStage.getLabel();
            }
            if (sourceLabel == null || nextLabel == null) {
                return null;
            }
            PreFingerprint preFingerprint = new PreFingerprint(this.filterFieldNames, stageIdentification, sourceLabel, nextLabel, -1, this);
            preFingerprint.setFingerprintCache(FingerprintCache.getInstance());
            return preFingerprint;
        } else {
            PreFingerprint preFingerprint = new PreFingerprint(this.filterFieldNames, stageIdentification, "0", "0", -1, this);
            preFingerprint.setFingerprintCache(FingerprintCache.getInstance());
            return preFingerprint;
        }
    }

    /**
     * 分析被过滤的原因
     *
     * @param message
     * @param context
     * @return
     */
    private NotFireReason createNotReason(T message, AbstractContext<T> context) {
        if (getPipeline().getHomologousOptimization() != null) {
            PreFingerprint preFingerprint = getPreFingerprint();
            if (preFingerprint == null) {
                return null;
            }
            String msgKey = preFingerprint.createFieldMsg(message);
            NotFireReason notFireReason = getPipeline().getHomologousOptimization().analysisNotFireReason((FilterChainStage) this, msgKey, context.getNotFireExpressionMonitor());
            notFireReason.analysis(message);
            return notFireReason;
        }
        return null;
    }

    private JSONArray createJsonArray(List<T> msgs) {
        JSONArray jsonArray = new JSONArray();
        for (T msg : msgs) {
            jsonArray.add(msg.getMessageBody());
        }
        return jsonArray;
    }

    public List<String> getNextStageLabels() {
        return nextStageLabels;
    }

    public void setNextStageLabels(List<String> nextStageLabels) {
        this.nextStageLabels = nextStageLabels;
    }

    public List<String> getPrevStageLabels() {
        return prevStageLabels;
    }

    public void setPrevStageLabels(List<String> prevStageLabels) {
        this.prevStageLabels = prevStageLabels;
    }

    public String getMsgSourceName() {
        return msgSourceName;
    }

    public void setMsgSourceName(String msgSourceName) {
        this.msgSourceName = msgSourceName;
    }

    public AbstractPipeline<?> getPipeline() {
        return pipeline;
    }

    public void setPipeline(AbstractPipeline<?> pipeline) {
        this.pipeline = pipeline;
    }

    public String getOwnerSqlNodeTableName() {
        return ownerSqlNodeTableName;
    }

    public void setOwnerSqlNodeTableName(String ownerSqlNodeTableName) {
        this.ownerSqlNodeTableName = ownerSqlNodeTableName;
    }

    public StageMetric getStageMetric() {
        return stageMetric;
    }

    public Long calculateInCount() {
        return stageMetric.getInCount();
    }

    public double calculateInQPS() {
        return stageMetric.getQps();
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    public String getFilterFieldNames() {
        return filterFieldNames;
    }

    public void setFilterFieldNames(String filterFieldNames) {
        this.filterFieldNames = filterFieldNames;
    }

    public PreFingerprint getPreFingerprint() {
        return preFingerprint;
    }

    public void setPreFingerprint(PreFingerprint preFingerprint) {
        this.preFingerprint = preFingerprint;
    }

    public StageGroup getStageGroup() {
        return stageGroup;
    }

    public void setStageGroup(StageGroup stageGroup) {
        this.stageGroup = stageGroup;
    }
}
