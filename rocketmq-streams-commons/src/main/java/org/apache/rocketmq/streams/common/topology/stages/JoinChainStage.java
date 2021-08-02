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

    protected String leftPiplineName;
    protected String rightPiplineName;
    protected String rigthDependentTableName;

    protected transient ChainPipeline leftPipline;
    protected transient ChainPipeline rightPipline;

    protected transient IStageHandle handle = new IStageHandle() {

        @Override
        protected IMessage doProcess(IMessage message, AbstractContext context) {
            String lable = message.getHeader().getMsgRouteFromLable();
            String joinFlag = null;
            if (lable != null) {
                if (lable.equals(rigthDependentTableName)) {
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
                leftPipline.doMessage(message, context);
            } else {
                rightPipline.doMessage(message, context);
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
        leftPipline = configurableService.queryConfigurable(Pipeline.TYPE, leftPiplineName);
        rightPipline = configurableService.queryConfigurable(Pipeline.TYPE, rightPiplineName);
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
        if (IConfigurable.class.isInstance(window)) {
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

    public String getLeftPiplineName() {
        return leftPiplineName;
    }

    public void setLeftPiplineName(String leftPiplineName) {
        this.leftPiplineName = leftPiplineName;
    }

    public String getRightPiplineName() {
        return rightPiplineName;
    }

    public void setRightPiplineName(String rightPiplineName) {
        this.rightPiplineName = rightPiplineName;
    }

    public String getRigthDependentTableName() {
        return rigthDependentTableName;
    }

    public void setRigthDependentTableName(String rigthDependentTableName) {
        this.rigthDependentTableName = rigthDependentTableName;
    }

    public ChainPipeline getLeftPipline() {
        return leftPipline;
    }

    public void setLeftPipline(ChainPipeline leftPipline) {
        if (leftPipline != null) {
            this.leftPiplineName = leftPipline.getConfigureName();
        }
        this.leftPipline = leftPipline;
    }

    public ChainPipeline getRightPipline() {
        return rightPipline;
    }

    public void setRightPipline(ChainPipeline rightPipline) {
        if (rightPipline != null) {
            this.rightPiplineName = rightPipline.getConfigureName();
        }
        this.rightPipline = rightPipline;
    }
}
