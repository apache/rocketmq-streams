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

import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

public class JoinStartChainStage extends EmptyChainStage {
    protected String rightDependentTableName;
    protected String leftLabelName;
    protected String rightLabelName;

    @Override
    protected IMessage handleMessage(IMessage message, AbstractContext context) {
        String lable = message.getHeader().getMsgRouteFromLable();
        if (lable != null) {
            if (lable.equals(rightDependentTableName)) {
                message.getHeader().addRouteLabel(rightLabelName);
            } else {
                message.getHeader().addRouteLabel(leftLabelName);
            }

        } else {
            throw new RuntimeException("can not dipatch message, need route label " + toJson());
        }
        return message;
    }

    @Override public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        handleMessage(message, context);
        super.checkpoint(message, context, checkPointMessage);
    }

    @Override public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        handleMessage(message, context);
        super.addNewSplit(message, context, newSplitMessage);
    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        handleMessage(message, context);
        super.removeSplit(message, context, removeSplitMessage);
    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {
        handleMessage(message, context);
        super.batchMessageFinish(message, context, checkPointMessage);
    }

    public String getRightDependentTableName() {
        return rightDependentTableName;
    }

    public void setRightDependentTableName(String rightDependentTableName) {
        this.rightDependentTableName = rightDependentTableName;
    }

    public String getLeftLabelName() {
        return leftLabelName;
    }

    public void setLeftLabelName(String leftLabelName) {
        this.leftLabelName = leftLabelName;
    }

    public String getRightLabelName() {
        return rightLabelName;
    }

    public void setRightLabelName(String rightLabelName) {
        this.rightLabelName = rightLabelName;
    }
}
