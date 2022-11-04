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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.optimization.MessageGlobleTrace;
import org.apache.rocketmq.streams.common.optimization.fingerprint.PreFingerprint;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.utils.IdUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个pipeline会有一个固定的处理流程，通过stage组成。每个stage可以决定是否需要中断执行，也可以决定下个stage的输入参数
 *
 * @param <T> pipeline初期流转的对象，在流转过程中可能会发生变化
 */
public class Pipeline<T extends IMessage> extends BasedConfigurable implements IStreamOperator<T, T> {

    public static final Logger LOGGER = LoggerFactory.getLogger(Pipeline.class);

    public static final String TYPE = "pipeline";

    /**
     * pipeline name,通过配置配
     */
    protected transient String name;

    /**
     * stage列表
     */
    protected List<AbstractStage<?>> stages = new ArrayList<>();

    /**
     * 给数据源取个名字，主要用于同源任务归并,数据源名称，如果pipeline是数据源pipeline需要设置
     */
    private String sourceIdentification;
    /**
     * 主要用于在join，union场景，标记上游节点用
     */
    protected String msgSourceName;

    /**
     * KEY: source stage label value: key:next stage label: value :PreFingerprint
     */
    protected transient Map<String, Map<String, PreFingerprint>> preFingerprintExecutor = new HashMap<>();

    public Pipeline() {
        setType(TYPE);
    }

    @Override public T doMessage(T t, AbstractContext context) {
        return doMessage(t, context, null);
    }

    public T doMessage(T t, AbstractContext context, AbstractStage... replaceStage) {
        T message = doMessageInner(t, context, replaceStage);
        context.setMessage(message);
        return message;
    }

    /**
     * 可以替换某个阶段的阶段，而不用配置的阶段
     *
     * @param t
     * @param context
     * @param replaceStage
     * @return
     */
    protected T doMessageInner(T t, AbstractContext context, AbstractStage<?>... replaceStage) {
        return doMessageFromIndex(t, context, 0, replaceStage);
    }

    public T doMessageFromIndex(T t, AbstractContext context, int index, AbstractStage<?>... replaceStage) {
        context.setMessage(t);
        //boolean needFlush = needFlush(t);
        for (int i = index; i < stages.size(); i++) {
            AbstractStage<?> oriStage = stages.get(i);
            AbstractStage<?> stage = chooseReplaceStage(oriStage, replaceStage);
            boolean isContinue = executeStage(stage, t, context);
            if (!isContinue) {
                if (stage.isAsyncNode()) {
                    MessageGlobleTrace.finishPipeline(t);
                }
                return t;
            }
        }
        MessageGlobleTrace.finishPipeline(t);
        return t;
    }

    @Override protected boolean initConfigurable() {
        for (AbstractStage stage : stages) {
            stage.init();
        }
        return super.initConfigurable();
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

    /**
     * 执行一个stage
     *
     * @param stage
     * @param t
     * @param context
     */
    protected boolean executeStage(AbstractStage stage, T t, AbstractContext context) {
        if (t.getHeader().isSystemMessage()) {
            ISystemMessage systemMessage = t.getSystemMessage();
            if (systemMessage instanceof CheckPointMessage) {
                stage.checkpoint(t, context, (CheckPointMessage) systemMessage);
            } else if (systemMessage instanceof NewSplitMessage) {
                stage.addNewSplit(t, context, (NewSplitMessage) systemMessage);
            } else if (systemMessage instanceof RemoveSplitMessage) {
                stage.removeSplit(t, context, (RemoveSplitMessage) systemMessage);
            } else if (systemMessage instanceof BatchFinishMessage) {
                stage.batchMessageFinish(t, context, (BatchFinishMessage) systemMessage);
            } else {
                if (systemMessage == null) {
                    return true;
                }
                throw new RuntimeException("can not support this system message " + systemMessage.getClass().getName());
            }
            if (stage.isAsyncNode()) {
                context.breakExecute();
                return false;
            }
            return true;
        }
        context.resetIsContinue();
        if (context.isSplitModel() && !stage.isCloseSplitMode()) {
            List<T> oldSplits = context.getSplitMessages();
            List<T> newSplits = new ArrayList<T>();
            int splitMessageOffset = 0;
            T lastMsg = null;
            boolean isFinishTrace = MessageGlobleTrace.existFinishBranch(t);
            for (T subT : oldSplits) {
                context.closeSplitMode(subT);
                if (ChainPipeline.class.isInstance(this) && !((ChainPipeline) this).isTopology() && StringUtil.isNotEmpty(this.msgSourceName)) {
                    subT.getHeader().setMsgRouteFromLabel(t.getHeader().getMsgRouteFromLabel());
                } else {
                    subT.getHeader().setMsgRouteFromLabel(t.getHeader().getMsgRouteFromLabel());
                }

                subT.getHeader().addLayerOffset(splitMessageOffset);
                splitMessageOffset++;
                boolean isContinue = doMessage(subT, stage, context);
                lastMsg = subT;
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
            MessageGlobleTrace.setResult(t, isFinishTrace);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
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
            if (stage.isCloseSplitMode()) {
                if (StringUtil.isNotEmpty(stage.getSplitDataFieldName())) {
                    List<T> msgs = context.getSplitMessages();
                    JSONArray jsonArray = createJsonArray(msgs);
                    t.getMessageBody().put(stage.getSplitDataFieldName(), jsonArray);
                }
                context.closeSplitMode(t);
            }
            Boolean isFinishTrace = MessageGlobleTrace.existFinishBranch(t);
            boolean isContinue = doMessage(t, stage, context);
            MessageGlobleTrace.setResult(t, isFinishTrace);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
            return isContinue;
        }
        return true;
    }

    public boolean isAsynNode() {
        //for(AbstractStage stage:stages){
        //    if(stage.supportRepeateMessageFilter()==false){
        //        return false;
        //    }
        //}
        return false;
    }

    //private void flushStage(AbstractStage stage, IMessage message, AbstractContext context) {
    //    stage.checkpoint(message, context);
    //}

    //protected boolean needFlush(T msg) {
    //    return msg.getHeader().isNeedFlush();
    //}

    private JSONArray createJsonArray(List<T> msgs) {
        JSONArray jsonArray = new JSONArray();
        for (T msg : msgs) {
            jsonArray.add(msg.getMessageBody());
        }
        return jsonArray;
    }

    /**
     * 可以给指定的stage，替换掉已有的stage
     *
     * @param oriStage
     * @param replaceStage
     * @return
     */
    protected AbstractStage chooseReplaceStage(AbstractStage oriStage, AbstractStage... replaceStage) {
        if (replaceStage == null) {
            return oriStage;
        }
        for (AbstractStage stage : replaceStage) {
            if (stage != null && stage.getName().equals(oriStage.getName())) {
                return stage;
            }
        }
        return oriStage;
    }

    private boolean doMessage(T t, AbstractStage stage, AbstractContext context) {
        Object result = stage.doMessage(t, context);
        return result != null && context.isContinue();
    }

    public void addStage(AbstractStage stage) {
        this.stages.add(stage);
    }

    public void setStageLable(AbstractStage stage, String lable) {
        stage.setLabel(lable);
    }

    public void setStages(List<AbstractStage<?>> stages) {
        this.stages = stages;
    }

    @Override public void destroy() {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] {} is destroy, release pipeline {}", IdUtil.instanceId(), getConfigureName(), getName());
        }
        //stages.clear();
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

    public String getMsgSourceName() {
        return msgSourceName;
    }

    public String getSourceIdentification() {
        return sourceIdentification;
    }

    public void setSourceIdentification(String sourceIdentification) {
        this.sourceIdentification = sourceIdentification;
    }

    public Map<String, Map<String, PreFingerprint>> getPreFingerprintExecutor() {
        return preFingerprintExecutor;
    }

    public void setMsgSourceName(String msgSourceName) {
        this.msgSourceName = msgSourceName;
    }
}
