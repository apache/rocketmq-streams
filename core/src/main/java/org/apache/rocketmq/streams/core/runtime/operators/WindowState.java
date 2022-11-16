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
package org.apache.rocketmq.streams.core.runtime.operators;


import com.fasterxml.jackson.core.JsonProcessingException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import org.apache.rocketmq.streams.core.util.Utils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * windowState data how to encode KV
 * <pre>
 * +-----------+---------------+-----------+-------------+
 * | Int(4)    | Int(4)        | key bytes | value bytes |
 * | key length| value length  |           |             |
 * +-----------+---------------+-----------+-------------+
 * </pre>
 */
public class WindowState<K, V> implements Serializable {
    private static final long serialVersionUID = 1669344441528746814L;
    private long recordEarliestTimestamp = Long.MAX_VALUE;
    private long recordLastTimestamp;
    private K key;
    private V value;
    private byte[] keyBytes;
    private byte[] valueBytes;
    private Class<?> keyClazz;
    private Class<?> valueClazz;

    //only for Serializer/Deserializer
    public WindowState() {
    }

    public WindowState(K key, V value, long recordLastTimestamp) throws JsonProcessingException {
        this.key = key;
        this.value = value;
        this.recordLastTimestamp = recordLastTimestamp;
        if (key != null) {
            this.keyBytes = Utils.object2Byte(key);
            this.keyClazz = key.getClass();
        }

        if (value != null) {
            this.valueBytes = Utils.object2Byte(value);
            this.valueClazz = value.getClass();
        }
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) throws JsonProcessingException {
        this.key = key;
        this.keyBytes = Utils.object2Byte(key);
        if (key != null) {
            this.keyClazz = key.getClass();
        }
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) throws JsonProcessingException {
        this.value = value;
        this.valueBytes = Utils.object2Byte(value);
        if (value != null) {
            this.valueClazz = value.getClass();
        }
    }

    public byte[] getKeyBytes() {
        return keyBytes;
    }

    public void setKeyBytes(byte[] keyBytes) {
        this.keyBytes = keyBytes;
    }

    public byte[] getValueBytes() {
        return valueBytes;
    }


    public void setValueBytes(byte[] valueBytes) {
        this.valueBytes = valueBytes;
    }

    @SuppressWarnings("unchecked")
    public Class<V> getValueClazz() {
        return (Class<V>) valueClazz;
    }

    public void setValueClazz(Class<?> valueClazz) {
        this.valueClazz = valueClazz;
    }

    public void setKeyClazz(Class<?> keyClazz) {
        this.keyClazz = keyClazz;
    }

    @SuppressWarnings("unchecked")
    public Class<K> getKeyClazz() {
        return (Class<K>) keyClazz;
    }

    public long getRecordEarliestTimestamp() {
        return recordEarliestTimestamp;
    }

    public void setRecordEarliestTimestamp(long recordEarliestTimestamp) {
        this.recordEarliestTimestamp = recordEarliestTimestamp;
    }

    public long getRecordLastTimestamp() {
        return recordLastTimestamp;
    }

    public void setRecordLastTimestamp(long recordLastTimestamp) {
        this.recordLastTimestamp = recordLastTimestamp;
    }

    private static final ByteBuf buf = Unpooled.buffer(16);
    public static byte[] windowState2Byte(WindowState<?, ?> state) throws Throwable {
        if (state == null) {
            return new byte[0];
        }

        Class<?> keyClazz = state.getKeyClazz();
        if (keyClazz == null) {
            keyClazz = state.getKey().getClass();
        }
        byte[] keyClazzBytes = keyClazz.getName().getBytes(StandardCharsets.UTF_8);

        byte[] keyBytes = state.getKeyBytes();
        if (keyBytes == null) {
            keyBytes = Utils.object2Byte(state.getKey());
        }

        Class<?> valueClazz = state.getValueClazz();
        if (valueClazz == null) {
            valueClazz = state.getValue().getClass();
        }
        byte[] valueClazzBytes = valueClazz.getName().getBytes(StandardCharsets.UTF_8);

        byte[] valueBytes = state.getValueBytes();
        if (valueBytes == null) {
            valueBytes = Utils.object2Byte(state.getValue());
        }


        int length = 4 + 8 + 8 + 4 + keyClazzBytes.length + 4 + keyBytes.length + 4 + valueClazzBytes.length + 4 + valueBytes.length;

        buf.writeInt(length);

        buf.writeLong(state.getRecordLastTimestamp());
        buf.writeLong(state.getRecordEarliestTimestamp());

        //key class
        buf.writeInt(keyClazzBytes.length);
        buf.writeBytes(keyClazzBytes);

        //key
        buf.writeInt(keyBytes.length);
        buf.writeBytes(keyBytes);

        //value class
        buf.writeInt(valueClazzBytes.length);
        buf.writeBytes(valueClazzBytes);

        //value
        buf.writeInt(valueBytes.length);
        buf.writeBytes(valueBytes);

        byte[] bytes = new byte[buf.readableBytes()];
        buf.readBytes(bytes);

        buf.clear();
        return bytes;
    }

    public static <K,V> WindowState<K,V> byte2WindowState(byte[] bytes) throws Throwable {
        ByteBuf byteBuf = Unpooled.wrappedBuffer(bytes);
        int totalLength = byteBuf.readInt();
        if (bytes.length < totalLength) {
            System.out.println("less than normal.");
        }

        long recordLastTimestamp = byteBuf.readLong();
        long recordEarliestTimestamp = byteBuf.readLong();

        //key class
        int keyClazzLength = byteBuf.readInt();
        ByteBuf buf = byteBuf.readBytes(keyClazzLength);
        byte[] keyClazzBytes = new byte[keyClazzLength];
        buf.readBytes(keyClazzBytes);
        //实例化
        String keyClassName = new String(keyClazzBytes, StandardCharsets.UTF_8);
        Class<?> keyClazz = Class.forName(keyClassName);

        //key
        int keyLength = byteBuf.readInt();
        ByteBuf keyBuf = byteBuf.readBytes(keyLength);
        byte[] keyBytes = new byte[keyLength];
        keyBuf.readBytes(keyBytes);

        //value class
        int valueClazzLength = byteBuf.readInt();
        ByteBuf valueClazzBuf = byteBuf.readBytes(valueClazzLength);
        byte[] valueClazzBytes = new byte[valueClazzLength];
        valueClazzBuf.readBytes(valueClazzBytes);
        //实例化
        String valueClassName = new String(valueClazzBytes, StandardCharsets.UTF_8);
        Class<?> valueClazz = Class.forName(valueClassName);

        //value
        int valueLength = byteBuf.readInt();
        ByteBuf valueBuf = byteBuf.readBytes(valueLength);
        byte[] valueBytes = new byte[valueLength];
        valueBuf.readBytes(valueBytes);

        WindowState<K, V> result = new WindowState<>();
        result.setRecordLastTimestamp(recordLastTimestamp);
        result.setRecordEarliestTimestamp(recordEarliestTimestamp);
        result.setKeyBytes(keyBytes);
        result.setValueBytes(valueBytes);
        result.setKeyClazz(keyClazz);
        result.setValueClazz(valueClazz);
        result.setKey(Utils.byte2Object(keyBytes, result.getKeyClazz()));
        result.setValue(Utils.byte2Object(valueBytes, result.getValueClazz()));

        byteBuf.release();

        return result;
    }
}
