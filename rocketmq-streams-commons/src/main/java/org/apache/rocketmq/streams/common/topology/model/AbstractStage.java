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

import com.alibaba.fastjson.JSONObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.component.ComponentCreator;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessageProcessor;
import org.apache.rocketmq.streams.common.optimization.fingerprint.FingerprintCache;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public abstract class AbstractStage<T extends IMessage> extends BasedConfigurable implements IStreamOperator<T, T>, ISystemMessageProcessor {
    protected String filterFieldNames;

    private static final Log LOG = LogFactory.getLog(AbstractStage.class);

    public static final String TYPE = "stage";

    protected transient String name;

    /**
     * 是否关闭拆分模式，多条日志会合并在一起，字段名为splitDataFieldName
     */
    protected boolean closeSplitMode = false;

    protected String splitDataFieldName;

    protected transient Pipeline<?> pipeline;

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
     * 前置指纹记录
     */
    protected transient PreFingerprint preFingerprint = null;

    public AbstractStage() {
        setType(TYPE);
    }

    @Override public T doMessage(T t, AbstractContext context) {
        try {
            TraceUtil.debug(t.getHeader().getTraceId(), "AbstractStage", label, t.getMessageBody().toJSONString());
        } catch (Exception e) {
            LOG.error("t.getMessageBody() parse error", e);
        }
        IStageHandle<T> handle = selectHandle(t, context);
        if (handle == null) {
            return t;
        }
        Object result = handle.doMessage(t, context);
        if (!context.isContinue() || result == null) {
            return (T) context.breakExecute();
        }
        return (T) result;
    }

    /**
     * 是否是异步节点，流程会在此节点终止，启动新线程开启后续流程
     *
     * @return
     */
    public abstract boolean isAsyncNode();

    /**
     * 复制一个新的stage，主要用于并发场景
     *
     * @return
     */
    protected abstract IStageHandle<T> selectHandle(T t, AbstractContext context);

    public String getName() {
        return name;
    }

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
            labels = new ArrayList<>();
            for (String tempLabel : this.nextStageLabels) {
                if (routeLabel.equals(tempLabel)) {
                    labels.add(tempLabel);
                }
            }
        }
        if (StringUtil.isNotEmpty(filterLabel)) {
            for (String tempLabel : this.nextStageLabels) {
                if (filterLabel.equals(label)) {
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
        String stageIdentification = MapKeyUtil.createKeyBySign(".", pipeline.getNameSpace(), pipeline.getConfigureName(), filterName);
        if (this.filterFieldNames == null) {
            this.filterFieldNames = ComponentCreator.getProperties().getProperty(stageIdentification);
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
            return new PreFingerprint(this.filterFieldNames, stageIdentification, sourceLabel, nextLabel, -1, this, FingerprintCache.getInstance());
        } else {
            return new PreFingerprint(this.filterFieldNames, stageIdentification, "0", "0", -1, this, FingerprintCache.getInstance());
        }
    }

    @Override public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {

    }

    public List<String> getNextStageLabels() {
        return nextStageLabels;
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

    public Pipeline<?> getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline<?> pipeline) {
        this.pipeline = pipeline;
    }

    public String getOwnerSqlNodeTableName() {
        return ownerSqlNodeTableName;
    }

    public void setOwnerSqlNodeTableName(String ownerSqlNodeTableName) {
        this.ownerSqlNodeTableName = ownerSqlNodeTableName;
    }

    public void setNextStageLabels(List<String> nextStageLabels) {
        this.nextStageLabels = nextStageLabels;
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
}
