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
import com.google.common.collect.Lists;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.impl.memory.MemorySource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.annotation.ConfigurableReference;
import org.apache.rocketmq.streams.common.configuration.ConfigurationKey;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IConfigurablePropertySetter;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.IUDF;
import org.apache.rocketmq.streams.common.metadata.MetaData;
import org.apache.rocketmq.streams.common.monitor.ConsoleMonitorManager;
import org.apache.rocketmq.streams.common.monitor.IMonitor;
import org.apache.rocketmq.streams.common.optimization.IHomologousCalculate;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.MessageTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.threadpool.ScheduleFactory;
import org.apache.rocketmq.streams.common.topology.metric.StageGroup;
import org.apache.rocketmq.streams.common.topology.stages.OutputChainStage;
import org.apache.rocketmq.streams.common.utils.DipperThreadLocalUtil;
import org.apache.rocketmq.streams.common.utils.FileUtil;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.PipelineHTMLUtil;
import org.apache.rocketmq.streams.common.utils.PrintUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;

;

/**
 * 数据流拓扑结构，包含了source 算子，sink
 */
public class ChainPipeline<T extends IMessage> extends AbstractPipeline<T> implements Serializable, Runnable, IConfigurablePropertySetter {

    private static final long serialVersionUID = -5189371682717444347L;
    @ConfigurableReference protected ISource<?> source;

    /**
     * 是否发布，默认为true，关闭发布时，此字段为false，pipeline启动时应判断此字段是否为true，status默认都为1，status为0代表pipeline已被删除
     */
    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);
    protected transient List<StageGroup> rootStageGroups = new ArrayList<>();
    /**
     * channel对应后续的stageName
     */
    protected List<String> channelNextStageLabel;
    /**
     * 数据源输入格式，主要用于日志指纹过滤，如果没有则不做优化
     */
    protected MetaData channelMetaData;
    /**
     * 为了图形化拓扑和监控使用
     */
    protected transient List<StageGroup> stageGroups = new ArrayList<>();
    protected String createTableSQL;
    protected IHomologousCalculate homologousCalculate;
    private String channelName;

    @Override protected boolean initConfigurable() {
        createStageMap();
        Map<String, StageGroup> stageGroupMap = createStageGroupMap();
        for (StageGroup stageGroup : this.stageGroups) {
            stageGroup.init(this.stageMap, stageGroupMap);
            if (stageGroup.getParent() == null && !this.rootStageGroups.contains(stageGroup)) {
                this.rootStageGroups.add(stageGroup);
            }
        }

        return super.initConfigurable();
    }

    /**
     * 启动一个channel，并给channel应用pipeline
     */

    public void startJob() {

        //可重入
        if (!hasStart.compareAndSet(false, true)) {
            return;
        }
        String instanceId = IdUtil.instanceId();
        try {
            startPipeline();

            final IStreamOperator<T, T> receiver = this;

            source.start((IStreamOperator<T, T>) (message, context) -> {
                if (!message.getHeader().isSystemMessage()) {
                    //如果没有前置数据源则从消息里面取延迟
                    //msg.put("__time__",message.getHeader().getEventMsgTime());
                    ConsoleMonitorManager.getInstance().reportChannel(ChainPipeline.this, source, message);
                }
                message.getHeader().setPipelineName(this.getName());

                //然后再执行正式逻辑，测试正式逻辑遇到表达是计算，会先从头部信息上去找，如果找到就直接返回，如果没有才进行正式的计算
                return receiver.doMessage(message, context);
            });

            if ("true".equals(configuration.getProperty(ConfigurationKey.MONITOR_PIPELINE_HTML_SWITCH))) {
                ScheduleFactory.getInstance().execute(getNameSpace() + "-" + getName() + "-monitor_switch_schedule", this, 0, 10, TimeUnit.SECONDS);
            }
            LOGGER.info("[{}][{}] Pipeline_Start_Success", instanceId, this.getName());
        } catch (Exception e) {
            this.hasStart.set(false);
            //pipeline启动失败日志输出
            LOGGER.error("[{}][{}] Pipeline_Start_Error", instanceId, this.getName(), e);
            throw e;
        }

    }

    /**
     * 可以替换某个阶段的阶段，而不用配置的阶段
     *
     * @param t       数据
     * @param context 上下文
     * @return stage执行后的结果
     */
    @Override public T doMessage(T t, AbstractContext<T> context) {
        if (homologousCalculate != null) {
            homologousCalculate.calculate(t, context);
        }

        if (!t.getHeader().isSystemMessage()) {
            MessageTrace.joinMessage(t);//关联全局监控器
        }
        context.setMessage(t);
        doNextStages(context, getMsgSourceName(), channelName, this.channelNextStageLabel);
        return t;
    }

    public boolean isTopology() {
        return isTopology(this.channelNextStageLabel);
    }

    public void doNextStages(AbstractContext context, String msgPrevSourceName, String currentLabel, List<String> nextStageLabel) {
        if (!isTopology(nextStageLabel)) {
            return;
        }
        String oriMsgPreSourceName = msgPrevSourceName;
        int size = nextStageLabel.size();
        for (String label : nextStageLabel) {
            AbstractContext copyContext = context;
            if (size > 1) {
                copyContext = context.copy();
            }
            T msg = (T) copyContext.getMessage();
            AbstractStage<?> oriStage = stageMap.get(label);
            if (oriStage == null) {
                if (stages != null && stages.size() > 0) {
                    synchronized (this) {
                        oriStage = stageMap.get(label);
                        if (oriStage == null) {
                            createStageMap();
                            oriStage = stageMap.get(label);
                        }
                    }
                }
                if (oriStage == null) {
                    LOGGER.warn("[{}][{}] expect stage named {}, but the stage is not exist", IdUtil.instanceId(), this.getName(), label);
                    continue;
                }
            }
            AbstractStage stage = oriStage;
            if (filterByPreFingerprint(msg, copyContext, currentLabel, label)) {
                continue;
            }

            if (!msg.getHeader().isSystemMessage()) {
                ConsoleMonitorManager.getInstance().reportInput(stage, msg);
            }

            //boolean needFlush = needFlush(msg);
            if (StringUtil.isNotEmpty(oriMsgPreSourceName)) {
                msg.getHeader().setMsgRouteFromLable(oriMsgPreSourceName);
            }
            boolean isContinue = stage.executeStage(msg, copyContext);

            if (!isContinue) {
                //只要执行到了window分支都不应该被过滤
                if (stage.isAsyncNode() && !msg.getHeader().isSystemMessage()) {
                    MessageTrace.finishPipeline(msg);
                }
            } else {
                if (!msg.getHeader().isSystemMessage()) {
                    ConsoleMonitorManager.getInstance().reportOutput(stage, msg, ConsoleMonitorManager.MSG_FLOWED, null);
                }
                if (stage instanceof AbstractChainStage) {
                    AbstractChainStage<?> chainStage = (AbstractChainStage<?>) stage;
                    String msgSourceName = chainStage.getMsgSourceName();
                    if (StringUtil.isNotEmpty(msgSourceName)) {
                        msgPrevSourceName = msgSourceName;
                    }
                }

                if (copyContext.isSplitModel()) {
                    List<IMessage> messageList = copyContext.getSplitMessages();
                    int splitMessageOffset = 0;
                    for (IMessage message : messageList) {
                        AbstractContext abstractContext = copyContext.copy();
                        abstractContext.closeSplitMode(message);
                        message.getHeader().setMsgRouteFromLable(msg.getHeader().getMsgRouteFromLable());
                        message.getHeader().addLayerOffset(splitMessageOffset);
                        splitMessageOffset++;
                        List<String> labels = stage.doRoute(message);
                        if (labels == null || labels.size() == 0) {
                            if (!message.getHeader().isSystemMessage()) {
                                MessageTrace.finishPipeline(message);
                            }
                            continue;
                        }
                        doNextStages(abstractContext, msgPrevSourceName, stage.getLabel(), labels);
                    }
                } else {
                    List<String> labels = stage.doRoute(msg);
                    if (labels == null || labels.size() == 0) {
                        if (!msg.getHeader().isSystemMessage()) {
                            MessageTrace.finishPipeline(msg);
                        }
                        continue;
                    }
                    doNextStages(copyContext, msgPrevSourceName, stage.getLabel(), labels);
                }
            }
        }
    }

    protected boolean filterByPreFingerprint(IMessage t, AbstractContext context, String sourceName, String nextLable) {
        PreFingerprint preFingerprint = getPreFingerprint(sourceName, nextLable);
        if (preFingerprint != null) {
            boolean isFilter = preFingerprint.filterByLogFingerprint(t);
            if (isFilter) {
                context.breakExecute();
                return true;
            }
        }
        return false;
    }

    public List<JSONObject> executeWithMsgs(List<JSONObject> msgs) {
        MemorySource memorySource = new MemorySource();
        memorySource.setReceiver(this);
        memorySource.setNameSpace(this.getNameSpace());
        memorySource.setName(this.getName());
        memorySource.init();
        this.setSource(memorySource);
        if (msgs == null) {
            return null;
        }
        int sinkCount = 0;
        for (AbstractStage<?> stage : getStages()) {
            if (stage instanceof OutputChainStage) {
                OutputChainStage<?> outputChainStage = (OutputChainStage<?>) stage;
                outputChainStage.setCallbackModel(true);
                sinkCount++;
            }
        }

        startPipeline();
        MessageFinishCallBack messageFinishCallBack = new MessageFinishCallBack();
        messageFinishCallBack.setSinkCount(sinkCount);
        for (JSONObject jsonObject : msgs) {
            memorySource.doReceiveMessage(jsonObject);
        }

        try {
            memorySource.sendCheckpoint(memorySource.getQueueId());
            memorySource.executeMessage((Message) BatchFinishMessage.create(messageFinishCallBack));
            List<JSONObject> result = messageFinishCallBack.get();
            if ("true".equals(configuration.getProperty(ConfigurationKey.MONITOR_PIPELINE_HTML_SWITCH))) {
                this.run();
            }
            return result;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            destroy();
        }
    }

    public ChainPipeline addChainStage(AbstractChainStage chainStage) {
        addStage(chainStage);
        return this;
    }

    public ISource<?> getSource() {
        return source;
    }

    public void setSource(ISource<?> source) {
        this.source = source;
        if (getNameSpace() == null) {
            setNameSpace(source.getNameSpace());
        }
        if (getChannelName() == null) {
            channelName = source.getName();
        }

    }

    public void startPipeline() {

        if (this.udfs != null) {
            for (IUDF iudf : this.udfs) {
                iudf.loadUDF();
            }
        }

        List<AbstractStage<?>> stages = this.getStages();
        for (AbstractStage<?> stage : stages) {
            stage.setConfiguration(this.configuration);
            stage.startJob();
        }

        Iterable<IHomologousOptimization> iterable = ServiceLoader.load(IHomologousOptimization.class);
        Iterator<IHomologousOptimization> it = iterable.iterator();
        if (it.hasNext()) {
            IHomologousOptimization homologousOptimization = it.next();
            homologousOptimization.optimizate(Lists.newArrayList(this));
            this.homologousOptimization = homologousOptimization;
            this.homologousOptimization.setPeFingerprintForPipeline(this);
            this.homologousCalculate = homologousOptimization.createHomologousCalculate();
        }

    }

    private Map<String, StageGroup> createStageGroupMap() {
        Map<String, StageGroup> map = new HashMap<>();
        for (StageGroup stageGroup : stageGroups) {
            map.put(stageGroup.getName(), stageGroup);
        }
        return map;
    }

    public Map<String, AbstractStage<?>> createStageMap() {
        for (AbstractStage<?> stage : getStages()) {
            stage.init();
            stageMap.put(stage.getLabel(), stage);
            stage.setPipeline(this);
        }
        return stageMap;
    }

    public List<String> getChannelNextStageLabel() {
        return channelNextStageLabel;
    }

    public void setChannelNextStageLabel(List<String> channelNextStageLabel) {
        this.channelNextStageLabel = channelNextStageLabel;
    }

    private IMonitor createPipelineMonitor() {

        IMonitor pipelineMonitorForChannel = DipperThreadLocalUtil.get();
        final IStreamOperator<T, T> receiver = this;
        if (pipelineMonitorForChannel == null) {
            //主要监控channel的启动
            pipelineMonitorForChannel = IMonitor.createMonitor(this);
        }
        return pipelineMonitorForChannel;

    }

    @Override public String toString() {
        String LINE = PrintUtil.LINE;
        StringBuilder sb = new StringBuilder();
        sb.append("###namespace=").append(getNameSpace()).append("###").append(LINE);
        if (source != null) {
            sb.append(source.toString()).append(LINE);
        }
        if (stages != null) {
            for (AbstractStage<?> stage : stages) {
                sb.append(stage.toString());
            }
        }
        return sb.toString();
    }

    @Override public void destroy() {
        super.destroy();
        if (source != null) {
            source.destroy();
            hasStart.set(false);
        }
        if (this.udfs != null) {
            for (IUDF udf : this.udfs) {
                udf.destroy();
            }
        }
        for (Map.Entry<String, AbstractStage<?>> entry : stageMap.entrySet()) {
            entry.getValue().stopJob();
        }
        if ("true".equals(configuration.getProperty(ConfigurationKey.MONITOR_PIPELINE_HTML_SWITCH))) {
            ScheduleFactory.getInstance().cancel(getNameSpace() + "-" + getName() + "-monitor_switch_schedule");
        }
    }

    public void addStageGroup(StageGroup stageGroup) {
        if (this.stageGroups != null) {
            this.stageGroups.add(stageGroup);
        }
    }

    protected boolean isTopology(List<String> nextStageLabel) {
        return nextStageLabel != null && nextStageLabel.size() != 0;
    }

    public Map<String, AbstractStage<?>> getStageMap() {
        return stageMap;
    }

    public void setStageMap(Map<String, AbstractStage<?>> stageMap) {
        this.stageMap = stageMap;
    }

    public Boolean getHasStart() {
        return hasStart.get();
    }

    public MetaData getChannelMetaData() {
        return channelMetaData;
    }

    public void setChannelMetaData(MetaData channelMetaData) {
        this.channelMetaData = channelMetaData;
    }

    public String getChannelName() {
        return channelName;
    }

    public void setChannelName(String channelName) {
        this.channelName = channelName;
    }

    public List<StageGroup> getStageGroups() {
        return stageGroups;
    }

    public void setStageGroups(List<StageGroup> stageGroups) {
        this.stageGroups = stageGroups;
    }

    public List<StageGroup> getRootStageGroups() {
        return rootStageGroups;
    }

    public void setRootStageGroups(List<StageGroup> rootStageGroups) {
        this.rootStageGroups = rootStageGroups;
    }

    public String getCreateTableSQL() {
        return createTableSQL;
    }

    public void setCreateTableSQL(String createTableSQL) {
        this.createTableSQL = createTableSQL;
    }

    @Override public void run() {
        String filePath = FileUtil.getJarPath();
        if (StringUtil.isEmpty(filePath)) {
            filePath = "/tmp";
        }
        filePath = filePath + File.separator + getName() + ".html";
        try {
            String html = PipelineHTMLUtil.createHTML(this);
            FileUtil.write(filePath, html);
            LOGGER.info("[{}][{}] create pipeline html success in {}", IdUtil.instanceId(), getName(), filePath);
        } catch (Exception e) {
            LOGGER.info("[{}][{}] create pipeline html error on {}", IdUtil.instanceId(), getName(), e.getMessage(), e);
        }

    }

    @Override public void setConfigurableProperty(IConfigurable newConfigurable) {
        if (newConfigurable.getClass().isAssignableFrom(ChainPipeline.class)) {
            ChainPipeline<?> newPipeline = (ChainPipeline<?>) newConfigurable;
            this.toObject(newPipeline.toJson());
        }
    }

}
