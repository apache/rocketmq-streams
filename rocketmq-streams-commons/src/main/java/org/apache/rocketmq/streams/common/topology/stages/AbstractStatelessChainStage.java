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
import org.apache.rocketmq.streams.common.topology.model.AbstractChainStage;

public abstract class AbstractStatelessChainStage<T extends IMessage> extends AbstractChainStage<T> {
    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        checkPointMessage.replyAnyone();
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {

    }

    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {

    }

    @Override public void startJob() {

    }

    @Override public void stopJob() {

    }

    @Override
    public void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage) {

    }
}
