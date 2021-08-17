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
package org.apache.rocketmq.streams.common.channel.source;

import com.alibaba.fastjson.JSONObject;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.disruptor.BufferFullFunction;
import org.apache.rocketmq.streams.common.disruptor.DisruptorEvent;
import org.apache.rocketmq.streams.common.disruptor.DisruptorEventFactory;
import org.apache.rocketmq.streams.common.disruptor.DisruptorProducer;

/**
 * 不可靠的消息源，如http，syslog，可以继承这个类。做了系统保护，如果消息发送太快，可能会出现丢失。
 */
public abstract class AbstractUnreliableSource extends AbstractBatchSource {
    private static final Log LOG = LogFactory.getLog(AbstractUnreliableSource.class);

    protected Boolean enableAsyncReceive = true;
    protected boolean isSingleType = false;//是否只有单个生产者，如果是，则为true

    private transient ExecutorService cachedThreadPool = null;
    private transient int bufferSize = 1024;
    private transient Disruptor<DisruptorEvent> disruptor;
    private transient DisruptorProducer<Message> disruptorProducer;
    private transient BufferFullFunction bufferFullFunction;
    protected transient boolean discard = false;//如果过快，直接丢弃。只有enableAsyncReceive生效时使用
    private transient EventHandler<DisruptorEvent> eventEventHandler;

    @Override
    protected boolean initConfigurable() {
        bufferSize = 1024;
        boolean discard = false;//如果过快，直接丢弃。只有enableAsyncReceive生效时使用
        return super.initConfigurable();
    }

    public AbstractUnreliableSource() {
        super();
        if (!enableAsyncReceive) {
            return;
        }
        cachedThreadPool = new ThreadPoolExecutor(maxThread, maxThread,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());
        ProducerType producerType = ProducerType.MULTI;
        if (isSingleType) {
            producerType = ProducerType.SINGLE;
        }
        disruptor = new Disruptor<>(new DisruptorEventFactory(), bufferSize, cachedThreadPool, producerType,
            new YieldingWaitStrategy());
        eventEventHandler = new MessageEventHandler();
        disruptor.handleEventsWith(eventEventHandler);
        disruptor.start();
        disruptorProducer = new DisruptorProducer<>(disruptor);
        //bufferFullFunction = data -> batchAdd((JSONObject)data);
        bufferFullFunction = new BufferFullFunction() {
            @Override
            public void process(Object data) {
                LOG.warn("discard data ");
            }
        };
    }

    @Override
    public AbstractContext executeMessage(Message channelMessage) {
        if (enableAsyncReceive) {
            disruptorProducer.publish(channelMessage, bufferFullFunction, discard);
            return null;
        } else {
            return executeMessageBySupper(channelMessage);
        }

    }

    @Override
    public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override
    public boolean supportOffsetRest() {
        return false;
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    /**
     * 收到消息发送出去，因为是不可靠队列，如果对象不是json，则用UserDefinedMessage处理
     *
     * @param t
     * @param <T>
     * @return
     */
    public <T> AbstractContext doUnreliableReceiveMessage(T t) {
        return super.doReceiveMessage(createJson(t));
    }

    @Override
    public AbstractContext doReceiveMessage(JSONObject message) {
        return doUnreliableReceiveMessage(message);
    }

    @Override
    protected void executeMessageAfterReceiver(Message channelMessage, AbstractContext context) {
    }

    public Boolean getEnableAsyncReceive() {
        return enableAsyncReceive;
    }

    public void setEnableAsyncReceive(Boolean enableAsyncReceive) {
        this.enableAsyncReceive = enableAsyncReceive;
    }

    public AbstractContext executeMessageBySupper(Message msg) {
        return super.executeMessage(msg);

    }

    protected class MessageEventHandler implements EventHandler<DisruptorEvent> {
        @Override
        public void onEvent(DisruptorEvent event, long sequence, boolean endOfBatch) throws Exception {
            // LOG.info("get event " + event);
            Message msg = (Message)event.getData();
            executeMessageBySupper(msg);
        }
    }

    public boolean isSingleType() {
        return isSingleType;
    }

    public void setSingleType(boolean singleType) {
        isSingleType = singleType;
    }
}
