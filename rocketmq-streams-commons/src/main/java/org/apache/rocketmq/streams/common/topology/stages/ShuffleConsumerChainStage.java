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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Context;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.SectionPipeline;
import org.apache.rocketmq.streams.common.topology.model.AbstractRule;
import org.apache.rocketmq.streams.common.topology.model.IStageHandle;
import org.apache.rocketmq.streams.common.topology.shuffle.ShuffleMQCreator;
import org.apache.rocketmq.streams.common.utils.CompressUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.common.utils.TraceUtil;

public class ShuffleConsumerChainStage <T extends IMessage, R extends AbstractRule> extends AbstractStatelessChainStage<T>  implements IAfterConfigurableRefreshListener {
    /**
     * 消息所属的window
     */
    protected transient String MSG_OWNER = "MSG_OWNER";
    protected static final String SHUFFLE_MESSAGES = "SHUFFLE_MESSAGES";
    private static final String SHUFFLE_TRACE_ID = "SHUFFLE_TRACE_ID";
    public static final String SHUFFLE_OFFSET = "SHUFFLE_OFFSET";


    private transient SectionPipeline lastStages;


    protected transient ISource consumer;
    protected String shuffleOwnerName;//shuffle 拥有者到名子，如是窗口，则是windowname+groupname+updateflag




    protected transient AtomicBoolean hasStart = new AtomicBoolean(false);
    public void startSource() {
        if (consumer == null) {
            return;
        }
        if (hasStart.compareAndSet(false, true)) {
            consumer.start(new IStreamOperator() {
                @Override public Object doMessage(IMessage message, AbstractContext context) {
                    sendMessage(message,context);
                    return message;
                }
            });
        }

    }


    /**
     * 接收到分片信息，如果是系统消息，做缓存刷新，否则把消息放入缓存，同时计算存储的有效性
     *
     * @param oriMessage
     * @param context
     * @return
     */

    protected transient AtomicLong COUNT = new AtomicLong(0);

    /**
     * send shuffle msg 2 last node
     * @param oriMessage
     * @param context
     */
    protected void sendMessage(IMessage oriMessage, AbstractContext context) {
        if (oriMessage.getHeader().isSystemMessage()) {
            lastStages.doMessage(oriMessage,context);
            return ;

        }
        if (oriMessage.getMessageBody().getBooleanValue(ShuffleProducerChainStage.IS_COMPRESSION_MSG)) {
            byte[] bytes = oriMessage.getMessageBody().getBytes(ShuffleProducerChainStage.COMPRESSION_MSG_DATA);
            String msgStr = CompressUtil.unGzip(bytes);
            oriMessage.setMessageBody(JSONObject.parseObject(msgStr));
        }
        /**
         * 过滤不是这个window的消息，一个shuffle通道，可能多个window共享，这里过滤掉非本window的消息
         */
        boolean isFilter = filterNotOwnerMessage(oriMessage);
        if (isFilter) {
            return;
        }
        String queueId = oriMessage.getHeader().getQueueId();
        JSONArray messages = oriMessage.getMessageBody().getJSONArray(SHUFFLE_MESSAGES);
        if (messages == null) {
            return;
        }

        String traceId = oriMessage.getMessageBody().getString(SHUFFLE_TRACE_ID);
        if (!StringUtil.isEmpty(traceId)) {
            TraceUtil.debug(traceId, "shuffle message in", "received message size:" + messages.size());
        }
        int i=0;
        for (Object obj : messages) {
            IMessage message = new Message((JSONObject) obj);
            message.getHeader().setQueueId(queueId);
            message.getHeader().setOffset(oriMessage.getHeader().getOffset());
            message.getHeader().addLayerOffset(i++);
            /**
             * create new offset
             */
            message.getMessageBody().put(SHUFFLE_OFFSET, oriMessage.getHeader().getOffset());
            lastStages.doMessage(message,new Context(message));
        }
    }

    /**
     * When the producer receives the first piece of data, it will notify the consumer to start consumption
     * @param t
     * @param context
     * @return
     */
    @Override protected IStageHandle<T> selectHandle(T t, AbstractContext context) {
        return new IStageHandle<T>() {
            @Override protected T doProcess(T t, AbstractContext context) {
                startSource();
                return null;
            }

            @Override public String getName() {
                return ShuffleConsumerChainStage.class.getName();
            }
        };
    }
    /**
     * 过滤掉不是这个window的消息
     *
     * @param oriMessage
     * @return
     */
    protected boolean filterNotOwnerMessage(IMessage oriMessage) {
        String owner = oriMessage.getMessageBody().getString(MSG_OWNER);
        if (owner != null && owner.equals(this.shuffleOwnerName)) {
            return false;
        }
        return true;
    }




    @Override public boolean isAsyncNode() {
        return false;
    }

    public String getShuffleOwnerName() {
        return shuffleOwnerName;
    }

    public void setShuffleOwnerName(String shuffleOwnerName) {
        this.shuffleOwnerName = shuffleOwnerName;
    }

    @Override public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if(this.consumer==null){
            this.consumer=ShuffleMQCreator.getSource(getPipeline().getNameSpace(),getPipeline().getConfigureName(),shuffleOwnerName);
            this.consumer.init();
            lastStages = getReceiverAfterCurrentNode();
        }


    }
}
