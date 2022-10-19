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
package org.apache.rocketmq.streams.core.serialization;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.common.Pair;

/**
 * shuffle data how to encode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+-------------+
 * </pre>
 */
public abstract class ShuffleProtocol {
    protected byte[] merge(byte[] keyBytes, byte[] valueBytes) {
        ByteBuf buf = ByteBufAllocator.DEFAULT.heapBuffer(16);
        buf.writeInt(keyBytes.length);
        buf.writeInt(valueBytes.length);
        buf.writeBytes(keyBytes);
        buf.writeBytes(valueBytes);

        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        return bytes;
    }

    protected Pair<byte[], byte[]> split(byte[] total) {
        ByteBuf byteBuf = Unpooled.copiedBuffer(total);

        int keyLength = byteBuf.readInt();
        int valueLength = byteBuf.readInt();
        ByteBuf keyByteBuf = byteBuf.readBytes(keyLength);
        ByteBuf valueByteBuf = byteBuf.readBytes(valueLength);

        byte[] keyBytes = new byte[keyByteBuf.readableBytes()];
        keyByteBuf.readBytes(keyBytes);

        byte[] valueBytes = new byte[valueByteBuf.readableBytes()];
        valueByteBuf.readBytes(valueBytes);

        return new Pair<>(keyBytes, valueBytes);
    }

}
