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

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.AbstractStage;
import org.apache.rocketmq.streams.common.topology.model.Pipeline;

/**
 * 流拓扑结构的一个节点
 */
public abstract class ChainStage<T extends IMessage> extends AbstractStage<T> {

    /**
     * 开发使用
     */
    @Changeable
    protected String entityName;

    /**
     * 是否取消IAfterConfigurableRefreshListener的执行，如成员变量通过set设置时，可以使用
     */
    protected boolean cancelAfterConfigurableRefreshListener = false;

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public boolean isCancelAfterConfigurableRefreshListener() {
        return cancelAfterConfigurableRefreshListener;
    }

    public void setCancelAfterConfigurableRefreshListener(boolean cancelAfterConfigurableRefreshListener) {
        this.cancelAfterConfigurableRefreshListener = cancelAfterConfigurableRefreshListener;
    }

    /**
     * 向pipelines传递系统消息，主要用于union，subpipline等场景
     *
     * @param message
     * @param context
     * @param pipelines
     */
    public void sendSystem(IMessage message, AbstractContext context, Collection<ChainPipeline<?>> pipelines) {
        if (!message.getHeader().isSystemMessage()) {
            return;
        }
        if (pipelines != null && pipelines.size() > 0) {
            for (ChainPipeline pipeline : pipelines) {
                pipeline.doMessage(message, context);
            }
        }
    }

    /**
     * 向pipelines传递系统消息，主要用于union，subpipline等场景
     *
     * @param message
     * @param context
     * @param pipelines
     */
    public void sendSystem(IMessage message, AbstractContext context, Pipeline... pipelines) {
        if (!message.getHeader().isSystemMessage() || pipelines == null) {
            return;
        }
        Set<ChainPipeline<?>> set = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            if (pipeline != null) {
                set.add((ChainPipeline) pipeline);
            }
        }
        sendSystem(message, context, set);
    }

    protected SectionPipeline getReceiverAfterCurrentNode() {
        return new SectionPipeline((ChainPipeline<?>) getPipeline(), this);
    }

}
