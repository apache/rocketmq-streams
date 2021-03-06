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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.stages.JoinChainStage;
import org.apache.rocketmq.streams.common.topology.stages.WindowChainStage;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;

/**
 * ?????????????????????????????????????????????????????????
 */
@Deprecated
public abstract class AbstractMutilPipelineChainPipline<T extends IMessage> extends ChainStage<T> implements IAfterConfigurableRefreshListener {
    /**
     * pipeline name??????????????????????????????????????????pipline??????????????????pipline name
     */
    protected List<String> piplineNames = new ArrayList<>();
    //??????pipline??????????????????????????????????????????????????????????????????name?????????name????????????
    protected Map<String, Set<String>> piplineName2MsgSourceName;

    /**
     * piplineNames???????????????
     */
    protected transient Map<String, ChainPipeline<?>> piplines = null;

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            if (CollectionUtil.isEmpty(piplines)) {
                return message;
            }
            String msgSourceName = message.getHeader().getMsgRouteFromLable();
            if (piplines.size() > 0) {
                List<IMessage> messages = new ArrayList<>();
                Iterator<Entry<String, Set<String>>> it = piplineName2MsgSourceName.entrySet().iterator();
                while (it.hasNext()) {
                    Entry<String, Set<String>> entry = it.next();
                    String piplineName = entry.getKey();
                    Set<String> values = entry.getValue();
                    for(String value:values){
                        if (msgSourceName != null && msgSourceName.equals(value)) {//??????????????????????????????map?????????????????????????????????????????????pipline
                            ChainPipeline pipline = piplines.get(piplineName);
                            IMessage copyMessage = message.deepCopy();
                            //copyMessage.getMessageBody().put(ORI_MESSAGE_KEY,message.getMessageBody());
                            // ??????????????????????????????????????????????????????????????????????????????
                            Context newContext = new Context(copyMessage);
                            copyMessage.getHeader().setMsgRouteFromLable(msgSourceName);
                            boolean needReturn = executePipline(pipline, copyMessage, newContext, msgSourceName);
                            if (needReturn) {
                                return message;
                            }
                            if (newContext.isContinue()) {
                                if (newContext.isSplitModel()) {
                                    messages.addAll(newContext.getSplitMessages());
                                } else {
                                    messages.add(copyMessage);
                                }

                            }
                        }
                    }

                }
                for (IMessage msg : messages) {
                    msg.getHeader().setMsgRouteFromLable(msgSourceName);
                }
                doMessageAfterFinishPipline(message, context, messages);
                return message;
            }
            ;
            return message;
        }

        @Override
        public String getName() {
            return AbstractMutilPipelineChainPipline.class.getName();
        }
    };

    /**
     * ???????????????pipline???????????????????????????????????????pipline????????????????????????true???????????????false
     *
     * @param copyMessage
     * @param newContext
     * @param msgSourceName
     * @return ?????????????????????pipline ??????true???????????????false
     */
    protected abstract boolean executePipline(ChainPipeline pipline, IMessage copyMessage, Context newContext, String msgSourceName);

    /**
     * ???????????????pipline?????????????????????????????????pipline???????????????????????????????????????
     *
     * @param message
     * @param context
     * @param messages
     */
    protected abstract void doMessageAfterFinishPipline(IMessage message, AbstractContext context, List<IMessage> messages);

    @Override
    public boolean isAsyncNode() {
        for (Pipeline pipline : piplines.values()) {
            if (pipline.isAsynNode() == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        sendSystem(message, context, piplines.values());
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        sendSystem(message, context, piplines.values());
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        sendSystem(message, context, piplines.values());
    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {
        sendSystem(message, context, piplines.values());
    }

    public void addPipline(ChainPipeline pipline) {
        this.piplineNames.add(pipline.getConfigureName());
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if (piplineNames == null) {
            return;
        }
        Map<String, ChainPipeline<?>> piplineMap = new HashMap<>();
        for (String pipeLineName : piplineNames) {
            ChainPipeline chainPipline = configurableService.queryConfigurable(Pipeline.TYPE, pipeLineName);
            if (chainPipline != null) {
                piplineMap.put(chainPipline.getConfigureName(), chainPipline);
            }
            List<AbstractStage<?>>  stages= chainPipline.getStages();
            for(AbstractStage stage:stages){
                stage.setPipeline(getPipeline());
                 if(WindowChainStage.class.isInstance(stage)){
                    ((WindowChainStage)stage).getWindow().setFireReceiver(getReceiverAfterCurrentNode());
                }else if(JoinChainStage.class.isInstance(stage)){
                     ((JoinChainStage) stage).doProcessAfterRefreshConfigurable(configurableService);
                     ((JoinChainStage)stage).getWindow().setFireReceiver(getReceiverAfterCurrentNode());
                 }
            }

        }
        this.piplines = piplineMap;
    }

    public List<String> getPiplineNames() {
        return piplineNames;
    }

    public void setPiplineNames(List<String> piplineNames) {
        this.piplineNames = piplineNames;
    }

    public List<ChainPipeline> getPiplines() {
        List<ChainPipeline> piplines = new ArrayList<>();
        piplines.addAll(this.piplines.values());
        return piplines;
    }

    public Map<String, Set<String>> getPiplineName2MsgSourceName() {
        return piplineName2MsgSourceName;
    }

    public void setPiplineName2MsgSourceName(
        Map<String, Set<String>> piplineName2MsgSourceName) {
        this.piplineName2MsgSourceName = piplineName2MsgSourceName;
    }

    public ChainPipeline getPipeline(String pipelineName){
        return this.piplines.get(pipelineName);
    }


}
