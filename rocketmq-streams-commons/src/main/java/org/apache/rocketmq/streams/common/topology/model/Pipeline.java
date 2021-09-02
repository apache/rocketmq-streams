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
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.BasedConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.optimization.MessageGloableTrace;
import org.apache.rocketmq.streams.common.utils.StringUtil;

/**
 * 每个pipline会有一个固定的处理流程，通过stage组成。每个stage可以决定是否需要中断执行，也可以决定下个stage的输入参数
 *
 * @param <T> pipline初期流转的对象，在流转过程中可能会发生变化
 */
public class Pipeline<T extends IMessage> extends BasedConfigurable implements IStreamOperator<T, T> {

    public static final Log LOG = LogFactory.getLog(Pipeline.class);

    public static final String TYPE = "pipeline";

    /**
     * pipeline name,通过配置配
     */
    protected transient String name;

    /**
     * stage列表
     */
    protected List<AbstractStage> stages = new ArrayList<>();

    //给数据源取个名字，主要用于同源任务归并
    private String sourceIdentification;//数据源名称，如果pipline是数据源pipline需要设置
    protected String msgSourceName;//主要用于在join，union场景，标记上游节点用

    public Pipeline() {
        setType(TYPE);
    }

    @Override
    public T doMessage(T t, AbstractContext context) {
        T message = doMessage(t, context, null);
        return message;
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
    protected T doMessageInner(T t, AbstractContext context, AbstractStage... replaceStage) {
        return doMessageFromIndex(t, context, 0, replaceStage);
    }

    public T doMessageFromIndex(T t, AbstractContext context, int index, AbstractStage... replaceStage) {
        context.setMessage(t);
        //boolean needFlush = needFlush(t);
        for (int i = index; i < stages.size(); i++) {
            AbstractStage oriStage = stages.get(i);
            AbstractStage stage = chooseReplaceStage(oriStage, replaceStage);
            boolean isContinue = executeStage(stage, t, context);
            if (!isContinue) {
                if (stage.isAsyncNode()) {
                    MessageGloableTrace.finishPipline(t);
                    ;
                }
                return t;
            }
        }
        MessageGloableTrace.finishPipline(t);
        return t;
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
                stage.checkpoint(t, context, (CheckPointMessage)systemMessage);
            } else if (systemMessage instanceof NewSplitMessage) {
                stage.addNewSplit(t, context, (NewSplitMessage)systemMessage);
            } else if (systemMessage instanceof RemoveSplitMessage) {
                stage.removeSplit(t, context, (RemoveSplitMessage)systemMessage);
            } else {
                if(systemMessage==null){
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
        if (context.isSplitModel() && stage.isCloseSplitMode() == false) {
            List<T> oldSplits = context.getSplitMessages();
            List<T> newSplits = new ArrayList<T>();
            int splitMessageOffset = 0;
            T lastMsg = null;
            for (T subT : oldSplits) {
                context.closeSplitMode(subT);
                subT.getHeader().setMsgRouteFromLable(t.getHeader().getMsgRouteFromLable());
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
            MessageGloableTrace.clear(t);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
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
            boolean isContinue = doMessage(t, stage, context);
            MessageGloableTrace.clear(t);//因为某些stage可能会嵌套pipline，导致某个pipline执行完成，这里把局部pipline带来的成功清理掉，所以不参与整体的pipline触发逻辑
            //if (needFlush) {
            //    flushStage(stage, t, context);
            //}
            if (!isContinue) {
                return false;
            }
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
        Object result = null;
        result = stage.doMessage(t, context);
        if (result == null || !context.isContinue()) {
            return false;
        }
        return true;
    }

    public void addStage(AbstractStage stage) {
        this.stages.add(stage);
    }

    public void setStageLable(AbstractStage stage, String lable) {
        stage.setLabel(lable);
    }

    public void setStages(List<AbstractStage> stages) {
        this.stages = stages;
    }

    @Override
    public void destroy() {
        if (LOG.isInfoEnabled()) {
            LOG.info(getName() + " is destroy, release pipline " + stages.size());
        }
        stages.clear();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<AbstractStage> getStages() {
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

    public void setMsgSourceName(String msgSourceName) {
        this.msgSourceName = msgSourceName;
    }
}
