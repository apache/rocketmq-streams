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
package org.apache.rocketmq.streams.common.topology.stages;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.configurable.annotation.ENVDependence;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;
import org.apache.rocketmq.streams.common.topology.task.StreamsTask;

public class
SubPiplineChainStage<T extends IMessage> extends ChainStage<T> implements IAfterConfigurableRefreshListener {

    private static final Log LOG = LogFactory.getLog(SubPiplineChainStage.class);

    @ENVDependence
    protected String filterMsgFieldNames;//需要去重的字段列表，用逗号分割
    @ENVDependence
    protected String filterMsgSwitch;//开启过滤的开关
    protected transient StreamsTask streamsTask;

    protected transient IStageHandle handle = new IStageHandle() {
        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            streamsTask.doMessage(message, context);
            return null;
        }

        @Override
        public String getName() {
            return SubPiplineChainStage.class.getName();
        }
    };

    @Override
    public boolean isAsyncNode() {
        for (Pipeline pipline : streamsTask.getPipelines()) {
            if (pipline.isAsynNode() == true) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        sendSystem(message, context, streamsTask.getPipelines());
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        sendSystem(message, context, streamsTask.getPipelines());
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        sendSystem(message, context, streamsTask.getPipelines());
    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {
        sendSystem(message, context, streamsTask.getPipelines());
    }

    /**
     * 每隔一段时间会重新刷新数据，如果有新增的pipline会加载起来，如果有删除的会去除掉
     *
     * @param configurableService
     */
    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if (streamsTask == null) {
            streamsTask = new StreamsTask();
            streamsTask.setNameSpace(getPipeline().getNameSpace());
            streamsTask.setConfigureName(getPipeline().getConfigureName());
            if (this.filterMsgSwitch != null && this.filterMsgSwitch.equals("true")) {
                streamsTask.setLogFingerprint(this.filterMsgFieldNames);
            }

            //            ChainPipeline chainPipeline=(ChainPipeline)getPipeline();
            //            streamsTask.setParallelTasks(chainPipeline.getSource().getMaxThread()-1);
            streamsTask.init();
        }
        streamsTask.doProcessAfterRefreshConfigurable(configurableService);
    }

    /**
     * 把消息投递给pipline的channel，让子pipline完成任务 注意：子pipline对消息的任何修改，都不反映到当前的pipline
     *
     * @param t
     * @param context
     * @return
     */
    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    public String getFilterMsgFieldNames() {
        return filterMsgFieldNames;
    }

    public void setFilterMsgFieldNames(String filterMsgFieldNames) {
        this.filterMsgFieldNames = filterMsgFieldNames;
    }

    public String getFilterMsgSwitch() {
        return filterMsgSwitch;
    }

    public void setFilterMsgSwitch(String filterMsgSwitch) {
        this.filterMsgSwitch = filterMsgSwitch;
    }
}
