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

import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageHeader;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.model.IWindow;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;

/**
 * 会在解析时，增加一个script（_join_flag='true'），在rigth/left pipline加一个window对应的stage
 */
public class JoinChainStage<T extends IMessage> extends AbstractWindowStage<T> {

    protected String leftPipelineName;
    protected String rightPipelineName;
    protected String rightDependentTableName;

    protected transient ChainPipeline leftPipeline;
    protected transient ChainPipeline rightPipeline;

    protected transient IStageHandle handle = new IStageHandle() {

        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            String lable = message.getHeader().getMsgRouteFromLable();
            String joinFlag = null;
            if (lable != null) {
                if (lable.equals(rightDependentTableName)) {
                    joinFlag = MessageHeader.JOIN_RIGHT;
                } else {
                    joinFlag = MessageHeader.JOIN_LEFT;
                }
                message.getHeader().setMsgRouteFromLable(joinFlag);
            } else {
                throw new RuntimeException("can not dipatch message, need route label " + toJson());
            }
            window.setFireReceiver(getReceiverAfterCurrentNode());
            if (MessageHeader.JOIN_LEFT.equals(joinFlag)) {
                leftPipeline.doMessage(message, context);
            } else {
                rightPipeline.doMessage(message, context);
            }
            //if(!MessageGloableTrace.existFinshBranch(message)){
            //    context.setBreak(true);
            //}
            context.breakExecute();
            return message;
        }

        @Override
        public String getName() {
            return JoinChainStage.class.getName();
        }

    };

    public JoinChainStage() {
        super.entityName = "join";
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        super.doProcessAfterRefreshConfigurable(configurableService);
        leftPipeline = configurableService.queryConfigurable(Pipeline.TYPE, leftPipelineName);
        rightPipeline = configurableService.queryConfigurable(Pipeline.TYPE, rightPipelineName);
    }

    @Override
    protected IStageHandle selectHandle(T t, AbstractContext context) {
        return handle;
    }

    public IWindow getWindow() {
        return window;
    }

    public void setWindow(IWindow window) {
        this.window = window;
        if (window instanceof IConfigurable) {
            setWindowName(window.getConfigureName());
            setLabel(window.getConfigureName());
        }
    }

    @Override
    public boolean isAsyncNode() {
        return true;
    }

    @Override
    public String getEntityName() {
        return super.entityName;
    }

    public String getLeftPipelineName() {
        return leftPipelineName;
    }

    public void setLeftPipelineName(String leftPipelineName) {
        this.leftPipelineName = leftPipelineName;
    }

    public String getRightPipelineName() {
        return rightPipelineName;
    }

    public void setRightPipelineName(String rightPipelineName) {
        this.rightPipelineName = rightPipelineName;
    }

    public String getRightDependentTableName() {
        return rightDependentTableName;
    }

    public void setRightDependentTableName(String rightDependentTableName) {
        this.rightDependentTableName = rightDependentTableName;
    }

    public ChainPipeline getLeftPipeline() {
        return leftPipeline;
    }

    public void setLeftPipeline(ChainPipeline leftPipeline) {
        if (leftPipeline != null) {
            this.leftPipelineName = leftPipeline.getConfigureName();
        }
        this.leftPipeline = leftPipeline;
    }

    public ChainPipeline getRightPipeline() {
        return rightPipeline;
    }

    public void setRightPipeline(ChainPipeline rightPipeline) {
        if (rightPipeline != null) {
            this.rightPipelineName = rightPipeline.getConfigureName();
        }
        this.rightPipeline = rightPipeline;
    }
}
