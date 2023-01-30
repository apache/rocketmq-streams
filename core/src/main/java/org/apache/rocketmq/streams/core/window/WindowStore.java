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
package org.apache.rocketmq.streams.core.window;


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

public class WindowStore<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(WindowStore.class.getName());

    private StateStore stateStore;
    private IdleWindowScaner idleWindowScaner;
    private ValueMapperAction<byte[], WindowState<K, V>> bytes2State;
    private ValueMapperAction<WindowState<K, V>, byte[]> state2Bytes;


    public WindowStore(StateStore stateStore,
                       ValueMapperAction<byte[], WindowState<K, V>> bytes2State,
                       ValueMapperAction<WindowState<K, V>, byte[]> state2Bytes,
                       IdleWindowScaner idleWindowScaner) {
        this.stateStore = stateStore;
        this.bytes2State = bytes2State;
        this.state2Bytes = state2Bytes;
        this.idleWindowScaner = idleWindowScaner;
    }

    public void put(MessageQueue stateTopicMessageQueue, WindowKey windowKey,
                    WindowState<K, V> value, BiConsumer<Long, String> function) throws Throwable {
        put(stateTopicMessageQueue, windowKey, value);
        this.idleWindowScaner.putNormalWindowCallback(windowKey, function);
    }

    public void put(MessageQueue stateTopicMessageQueue, WindowKey windowKey,
                    WindowState<K, V> value, Consumer<WindowKey> function) throws Throwable {
        put(stateTopicMessageQueue, windowKey, value);
        this.idleWindowScaner.putSessionWindowCallback(windowKey, function);
    }

    public void put(MessageQueue stateTopicMessageQueue, WindowKey windowKey,
                    WindowState<K, V> value, LongConsumer function) throws Throwable {
        put(stateTopicMessageQueue, windowKey, value);
        this.idleWindowScaner.putJoinWindowCallback(windowKey, function);
    }

    private void put(MessageQueue stateTopicMessageQueue, WindowKey windowKey, WindowState<K, V> value) throws Throwable {
        logger.debug("put key into store, key: " + windowKey);
        byte[] keyBytes = WindowKey.windowKey2Byte(windowKey);
        byte[] valueBytes = this.state2Bytes.convert(value);

        this.stateStore.put(stateTopicMessageQueue, keyBytes, valueBytes);
    }

    public WindowState<K, V> get(WindowKey windowKey) throws Throwable {
        byte[] bytes = WindowKey.windowKey2Byte(windowKey);
        byte[] valueBytes = this.stateStore.get(bytes);
        return deserializerState(valueBytes);
    }

    public List<Pair<WindowKey, WindowState<K, V>>> searchLessThanWatermark(String operatorName, long lessThanThisTime) throws Throwable {
        List<Pair<byte[], byte[]>> windowStateBytes = this.stateStore.searchStateLessThanWatermark(operatorName, lessThanThisTime, WindowKey::byte2WindowKey);

        List<Pair<WindowKey, WindowState<K, V>>> pairs = deserializerState(windowStateBytes);
        if (pairs.size() != 0) {
            logger.debug("exist window need to fire, operator:{}, windowEnd < {}", operatorName, lessThanThisTime);
        }

        return pairs;
    }

    public List<Pair<WindowKey, WindowState<K, V>>> searchMatchKeyPrefix(String operatorName) throws Throwable {
        List<Pair<byte[], byte[]>> pairs = this.stateStore.searchStateLessThanWatermark(operatorName, Long.MAX_VALUE, WindowKey::byte2WindowKey);

        return deserializerState(pairs);
    }

    public void deleteByKey(WindowKey windowKey) throws Throwable {
        if (windowKey == null) {
            return;
        }
        byte[] keyBytes = WindowKey.windowKey2Byte(windowKey);
        this.stateStore.delete(keyBytes);
        this.idleWindowScaner.removeWindowKey(windowKey);
    }

    private List<Pair<WindowKey, WindowState<K, V>>> deserializerState(List<Pair<byte[], byte[]>> windowStateBytes) throws Throwable {
        List<Pair<WindowKey, WindowState<K, V>>> result = new ArrayList<>();
        if (windowStateBytes == null || windowStateBytes.size() == 0) {
            return result;
        }


        for (Pair<byte[], byte[]> pair : windowStateBytes) {
            byte[] keyBytes = pair.getKey();
            WindowKey key = WindowKey.byte2WindowKey(keyBytes);
            WindowState<K, V> state = this.deserializerState(pair.getValue());

            Pair<WindowKey, WindowState<K, V>> temp = new Pair<>(key, state);
            result.add(temp);
        }
        return result;
    }

    private WindowState<K, V> deserializerState(byte[] source) throws Throwable {
        if (source == null) {
            return null;
        }

        return this.bytes2State.convert(source);
    }

}
