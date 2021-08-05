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

import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointState;
import org.apache.rocketmq.streams.common.configurable.IAfterConfiguableRefreshListerner;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.ChainStage;
import org.apache.rocketmq.streams.common.topology.model.IWindow;

public abstract class AbstractWindowStage<T extends IMessage> extends ChainStage<T> implements
    IAfterConfiguableRefreshListerner {
    protected String windowName;
    protected transient IWindow window;

    @Override
    public void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage) {
        if(message.getHeader().isNeedFlush()){
            if(message.getHeader().getCheckpointQueueIds()!=null&&message.getHeader().getCheckpointQueueIds().size()>0){
                window.getWindowCache().flush();
            }else {
                Set<String> queueIds=new HashSet<>();
                queueIds.add(message.getHeader().getQueueId());
                window.getWindowCache().flush();
            }

        }
        CheckPointState checkPointState=  new CheckPointState();
        checkPointState.setQueueIdAndOffset(window.getWindowCache().getFinishedQueueIdAndOffsets(checkPointMessage));
        checkPointMessage.reply(checkPointState);
    }

    @Override
    public void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage) {
        //do nothigh
    }
    @Override
    public void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage) {
        //if(message.getHeader().isNeedFlush()){
        //    if(message.getHeader().getCheckpointQueueIds()!=null&&message.getHeader().getCheckpointQueueIds().size()>0){
        //        window.getWindowCache().flush(message.getHeader().getCheckpointQueueIds());
        //    }else {
        //        Set<String> queueIds=new HashSet<>();
        //        queueIds.add(message.getHeader().getQueueId());
        //        window.getWindowCache().flush(queueIds);
        //    }
        //
        //}
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        window = configurableService.queryConfigurable(IWindow.TYPE, windowName);
        PiplineRecieverAfterCurrentNode receiver = getReceiverAfterCurrentNode();
        window.setFireReceiver(receiver);
    }

    public String getWindowName() {
        return windowName;
    }

    public void setWindowName(String windowName) {
        this.windowName = windowName;
    }

}
