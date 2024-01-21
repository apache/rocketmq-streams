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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.IUDF;
import org.apache.rocketmq.streams.common.optimization.IHomologousOptimization;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个pipeline会有一个固定的处理流程，通过stage组成。每个stage可以决定是否需要中断执行，也可以决定下个stage的输入参数
 *
 * @param <T> pipeline初期流转的对象，在流转过程中可能会发生变化
 */
public abstract class AbstractPipeline<T extends IMessage> extends BasedConfigurable implements IStreamOperator<T, T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(AbstractPipeline.class);

    public static final String TYPE = "pipeline";

    /**
     * pipeline name,通过配置配
     */
    protected transient String name;
    protected transient Map<String, AbstractStage<?>> stageMap = new HashMap<>();
    /**
     * stage列表
     */
    protected List<AbstractStage<?>> stages = new ArrayList<>();
    /**
     * 主要用于在join，union场景，标记上游节点用
     */
    protected String msgSourceName;
    protected List<IUDF> udfs = new ArrayList<>();
    /**
     * KEY: source stage label value: key:next stage label: value :PreFingerprint
     */
    protected transient Map<String, Map<String, PreFingerprint>> preFingerprintExecutor = new HashMap<>();
    protected transient ExecutorService executeTasks;
    protected IHomologousOptimization homologousOptimization;
    /**
     * 给数据源取个名字，主要用于同源任务归并,数据源名称，如果pipeline是数据源pipeline需要设置
     */
    private String sourceIdentification;

    public AbstractPipeline() {
        setType(TYPE);
    }

    public void addStage(AbstractStage stage) {
        this.stages.add(stage);
    }

    /**
     * regist pre filter Fingerprint
     *
     * @param preFingerprint
     */
    protected void registPreFingerprint(PreFingerprint preFingerprint) {
        if (preFingerprint == null) {
            return;
        }
        Map<String, PreFingerprint> preFingerprintMap = this.preFingerprintExecutor.computeIfAbsent(preFingerprint.getSourceStageLabel(), k -> new HashMap<>());
        preFingerprintMap.put(preFingerprint.getNextStageLabel(), preFingerprint);
    }

    protected PreFingerprint getPreFingerprint(String currentLable, String nextLable) {
        Map<String, PreFingerprint> preFingerprintMap = this.preFingerprintExecutor.get(currentLable);
        if (preFingerprintMap == null) {
            return null;
        }
        return preFingerprintMap.get(nextLable);
    }

    @Override public void destroy() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}][{}] Pipeline_Stop_Success", IdUtil.instanceId(), this.getName());
        }
        if (executeTasks != null) {
            this.executeTasks.shutdown();
        }
    }

    protected String createPipelineMonitorName() {
        return MapKeyUtil.createKeyBySign(".", getType(), getNameSpace(), this.getName());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<AbstractStage<?>> getStages() {
        return stages;
    }

    public void setStages(List<AbstractStage<?>> stages) {
        this.stages = stages;
    }

    public String getMsgSourceName() {
        return msgSourceName;
    }

    public void setMsgSourceName(String msgSourceName) {
        this.msgSourceName = msgSourceName;
    }

    public String getSourceIdentification() {
        return sourceIdentification;
    }

    public void setSourceIdentification(String sourceIdentification) {
        this.sourceIdentification = sourceIdentification;
    }

//    public Map<String, Map<String, PreFingerprint>> getPreFingerprintExecutor() {
//        return preFingerprintExecutor;
//    }

    public List<IUDF> getUdfs() {
        return udfs;
    }

    public void setUdfs(List<IUDF> udfs) {
        this.udfs = udfs;
    }

    public void setPreFingerprintExecutor(
        Map<String, Map<String, PreFingerprint>> preFingerprintExecutor) {
        this.preFingerprintExecutor = preFingerprintExecutor;
    }

    public IHomologousOptimization getHomologousOptimization() {
        return homologousOptimization;
    }
}
