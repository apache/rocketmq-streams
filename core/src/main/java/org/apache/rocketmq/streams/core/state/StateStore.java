package org.apache.rocketmq.streams.core.state;
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

import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.exception.RecoverStateStoreThrowable;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.List;
import java.util.Set;

public interface StateStore extends AutoCloseable {
    void init() throws Throwable;


    //addQueues    messageQueue of source topic,removeQueues messageQueue of source topic
    void recover(Set<MessageQueue> addQueues, Set<MessageQueue> removeQueues) throws Throwable;


    //messageQueue check the state of source topic is ok, wait if not.
    void waitIfNotReady(MessageQueue messageQueue) throws RecoverStateStoreThrowable;


    byte[] get(byte[] key) throws Throwable;

    byte[] get(String columnFamily, byte[] key) throws Throwable;

    void put(MessageQueue stateTopicMessageQueue, byte[] key, byte[] value) throws Throwable;

    void put(MessageQueue stateTopicMessageQueue, String columnFamily, byte[] key, byte[] value) throws Throwable;

    List<Pair<byte[], byte[]>> searchStateLessThanWatermark(String operatorName, long lessThanThisTime, ValueMapperAction<byte[], WindowKey> deserializer) throws Throwable;


    List<Pair<String, byte[]>> searchByKeyPrefix(String keyPrefix, ValueMapperAction<String, byte[]> string2Bytes, ValueMapperAction<byte[], String> byte2String) throws Throwable;

    void delete(byte[] key) throws Throwable;

    void persist(Set<MessageQueue> messageQueue) throws Throwable;
}
