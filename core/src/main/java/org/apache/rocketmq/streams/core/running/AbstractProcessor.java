package org.apache.rocketmq.streams.core.running;
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


import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.exception.RecoverStateStoreThrowable;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.metadata.StreamConfig;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class AbstractProcessor<T> implements Processor<T> {
    private final List<Processor<T>> children = new ArrayList<>();
    protected StreamContext<T> context;
    protected long allowDelay = 0;

    @Override
    public void addChild(Processor<T> processor) {
        children.add(processor);
    }

    @Override
    public void preProcess(StreamContext<T> context) throws RecoverStateStoreThrowable {
        this.context = context;
        this.context.init(getChildren());
        Object delayObj = this.context
                .getUserProperties()
                .getOrDefault(StreamConfig.ALLOW_LATENESS_MILLISECOND, StreamConfig.DEFAULT_ALLOW_LATE_MILLISECONDS);

        this.allowDelay = Long.parseLong(String.valueOf(delayObj));
    }

    protected List<Processor<T>> getChildren() {
        return Collections.unmodifiableList(children);
    }

    protected StateStore waitStateReplay() throws RecoverStateStoreThrowable {
        MessageQueue sourceTopicQueue = new MessageQueue(context.getSourceTopic(), context.getSourceBrokerName(), context.getSourceQueueId());

        StateStore stateStore = context.getStateStore();
        stateStore.waitIfNotReady(sourceTopicQueue);
        return stateStore;
    }

    @SuppressWarnings("unchecked")
    protected <KEY> Data<KEY, T> convert(Data<?, ?> data) {
        return (Data<KEY, T>) new Data<>(data.getKey(), data.getValue(), data.getTimestamp(), data.getHeader());
    }



    private final ByteBuf buf = Unpooled.buffer(16);
    /**
     * encode
     * <pre>
     * +-----------+---------------+-------------+-------------+
     * | Int(4)    |   className  | Int(4)       | value bytes |
     * | classname |              |object length |             |
     * +-----------+--------------+---------------+-------------+
     * </pre>
     * @param obj the object to serialize;
     * @return byte[] the result of serialize
     * @throws JsonProcessingException serialize exception.
     */
    protected byte[] object2Byte(Object obj) throws JsonProcessingException {
        if (obj == null) {
            return new byte[]{};
        }

        String name = obj.getClass().getName();
        byte[] className = name.getBytes(StandardCharsets.UTF_8);
        byte[] objBytes = Utils.object2Byte(obj);


        buf.writeInt(className.length);
        buf.writeBytes(className);
        buf.writeInt(objBytes.length);
        buf.writeBytes(objBytes);


        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        buf.clear();
        return bytes;
    }

    /**
     * decode
     * <pre>
     * +-----------+---------------+-------------+-------------+
     * | Int(4)    |   className  | Int(4)       | value bytes |
     * | classname |              |object length |             |
     * +-----------+--------------+---------------+-------------+
     * </pre>
     * @param bytes the byte array to deserialize;
     * @return V the result of deserialize
     * @throws Throwable deserialize exception.
     */
    @SuppressWarnings("unchecked")
    public <V> V byte2Object(byte[] bytes) throws Throwable {
        if (bytes == null || bytes.length == 0) {
            return null;
        }

        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);

        int classNameLength = byteBuf.readInt();
        ByteBuf classNameBuf = byteBuf.readBytes(classNameLength);

        byte[] clazzNameBytes = new byte[classNameBuf.readableBytes()];
        classNameBuf.readBytes(clazzNameBytes);
        //实例化
        String className = new String(clazzNameBytes, StandardCharsets.UTF_8);
        Class<V> clazz = (Class<V>)Class.forName(className);

        int objectLength = byteBuf.readInt();
        ByteBuf objBuf = byteBuf.readBytes(objectLength);
        byte[] objectBytes = new byte[objectLength];
        objBuf.readBytes(objectBytes);

        classNameBuf.release();
        objBuf.release();

        return Utils.byte2Object(objectBytes, clazz);
    }

    protected String toHexString(Object source) throws JsonProcessingException {
        if (source == null) {
            return null;
        }
        if (source instanceof String) {
            return (String) source;
        }
        byte[] sourceByte = this.object2Byte(source);

        return Utils.toHexString(sourceByte);
    }
}
