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

import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.AbstractMutilPipelineChainPipline;
import org.apache.rocketmq.streams.common.topology.ChainPipeline;

import java.util.List;

/**
 * 会把消息复制后转给其他的pipeline 主要处理场景是类似blink多任务过滤部分抽取成规则放到一个任务的场景
 */
public class UnionChainStage<T extends IMessage> extends AbstractMutilPipelineChainPipline<T> {

    private static final long serialVersionUID = -6448769339534974034L;

    @Override
    protected boolean executePipline(ChainPipeline pipline, IMessage copyMessage, Context newContext, String msgSourceName) {

        pipline.doMessage(copyMessage, newContext);
        return false;
    }

    @Override
    protected void doMessageAfterFinishPipline(IMessage message, AbstractContext context, List<IMessage> messages) {
        if (messages.size() == 0) {
            context.breakExecute();
        } else {
            context.openSplitModel();
            context.setSplitMessages(messages);
        }
    }

}
