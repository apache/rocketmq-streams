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
package org.apache.rocketmq.streams.common.channel.impl.memory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.channel.AbstractChannel;
import org.apache.rocketmq.streams.common.channel.sink.AbstractSink;
import org.apache.rocketmq.streams.common.channel.sink.ISink;
import org.apache.rocketmq.streams.common.channel.source.AbstractUnreliableSource;
import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.context.IMessage;

/**
 * 消息产生的source数据，就是通过sink写入的消息
 */
public class MemoryChannel extends AbstractChannel {
    /**
     * 是否启动qps的统计
     */
    protected transient volatile boolean startQPSCount = false;
    /**
     * 总处理数据数
     */
    protected transient AtomicLong COUNT = new AtomicLong(0);
    /**
     * 最早的处理时间
     */
    protected transient long firstReceiveTime = System.currentTimeMillis();

    public void setStartQPSCount(boolean startQPSCount) {
        this.startQPSCount = startQPSCount;
    }

    @Override
    protected ISink createSink() {
        return new AbstractSink() {
            @Override
            protected boolean batchInsert(List<IMessage> messages) {
                if (startQPSCount) {
                    long count = COUNT.addAndGet(messages.size());
                    long second = ((System.currentTimeMillis() - firstReceiveTime) / 1000);
                    double qps = count / second;
                    System.out.println("qps is " + qps + "。the count is " + count + ".the process time is " + second);
                }
                for (IMessage msg : messages) {
                    ((AbstractUnreliableSource) source).doUnreliableReceiveMessage(msg.getMessageValue());
                }
                return true;
            }
        };
    }

    @Override
    protected ISource createSource() {
        return new AbstractUnreliableSource() {
            @Override
            protected boolean startSource() {
                return super.startSource();
            }

            @Override public List<ISplit<?, ?>> getAllSplits() {
                return null;
            }
        };
    }

    @Override
    public String createCheckPointName() {
        return "memory-source";
    }

}
