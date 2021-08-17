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
import java.util.List;
import java.util.Set;
import org.apache.rocketmq.streams.common.configurable.IConfigurableIdentification;
import org.apache.rocketmq.streams.common.configurable.annotation.Changeable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
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
    protected boolean cancelAfterConfigurableRefreshListerner = false;

    public String getEntityName() {
        return entityName;
    }

    public void setEntityName(String entityName) {
        this.entityName = entityName;
    }

    public boolean isCancelAfterConfigurableRefreshListerner() {
        return cancelAfterConfigurableRefreshListerner;
    }

    public void setCancelAfterConfigurableRefreshListerner(boolean cancelAfterConfigurableRefreshListerner) {
        this.cancelAfterConfigurableRefreshListerner = cancelAfterConfigurableRefreshListerner;
    }

    /**
     * 获取当前节点后续pipeline节点
     *
     * @return
     */
    public PiplineRecieverAfterCurrentNode getReceiverAfterCurrentNode() {
        ChainPipeline pipeline = (ChainPipeline)getPipeline();

        return new PiplineRecieverAfterCurrentNode(pipeline);
    }

    /**
     * 执行当前后续的节点
     */
    public class PiplineRecieverAfterCurrentNode implements IStreamOperator<IMessage, AbstractContext<IMessage>>,
        IConfigurableIdentification {
        protected ChainPipeline pipeline;

        public PiplineRecieverAfterCurrentNode(ChainPipeline pipeline) {
            this.pipeline = pipeline;
        }

        public PiplineRecieverAfterCurrentNode() {

        }

        @Override
        public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
            //设置window触发后需要执行的逻辑
            if (pipeline.isTopology()) {
                pipeline.doNextStages(context, getMsgSourceName(), getNextStageLabels(), getOwnerSqlNodeTableName());
                return context;

            } else {
                final int index = chooseWindowStageNextIndex(pipeline);

                pipeline.doMessageFromIndex(message, context, index);
                return context;
            }
        }

        public ChainPipeline getPipeline() {
            return pipeline;
        }

        @Override
        public String getConfigureName() {
            return pipeline.getConfigureName();
        }

        @Override
        public String getNameSpace() {
            return pipeline.getNameSpace();
        }

        @Override
        public String getType() {
            return pipeline.getType();
        }
    }

    /**
     * 向pipelines传递系统消息，主要用于union，subpipline等场景
     *
     * @param message
     * @param context
     * @param pipelines
     */
    public void sendSystem(IMessage message, AbstractContext context, Collection<ChainPipeline> pipelines) {
        if (message.getHeader().isSystemMessage() == false) {
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
        if (message.getHeader().isSystemMessage() == false || pipelines == null) {
            return;
        }
        Set<ChainPipeline> set = new HashSet<>();
        for (Pipeline pipeline : pipelines) {
            set.add((ChainPipeline)pipeline);
        }
        sendSystem(message, context, set);
    }

    /**
     * 选择当前stage所在的位置
     *
     * @param pipeLine
     * @return
     */
    protected int chooseWindowStageNextIndex(ChainPipeline pipeLine) {
        List<AbstractStage<?>> stages = pipeLine.getStages();
        for (int i = 0; i < stages.size(); i++) {
            AbstractStage stage = stages.get(i);
            if (isCurrentWindowStage(stage)) {
                return (i + 1);
            }
        }
        return -1;
    }

    /**
     * 判断stage是否是当前stage
     *
     * @param stage
     * @return
     */

    protected boolean isCurrentWindowStage(AbstractStage stage) {
        return this.equals(stage);
    }

}
