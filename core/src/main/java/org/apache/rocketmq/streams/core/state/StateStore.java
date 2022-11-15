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

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.runtime.operators.WindowKey;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.List;
import java.util.Set;

public interface StateStore extends AutoCloseable {
    void init() throws Throwable;

    RocksDBStore getRocksDBStore();

    /**
     * @param addQueues    messageQueue of source topic
     * @param removeQueues messageQueue of source topic
     * @throws Throwable
     */
    void recover(Set<MessageQueue> addQueues, Set<MessageQueue> removeQueues) throws Throwable;


    /**
     * @param messageQueue 检查source topic中该queue的状态是否已经加载好，如果没有加载好，等待加载
     * @param key          可以不传入，使用messageQueue即可检查是否该queue的状态被恢复。
     *                     即将使用这个key get/put，将该key放入与state topic queue形成映射，为后续使用queue清理状态做准备。
     *                     多数情况下，recover时已经形成stateTopicQueue-key的映射，但是在处理数据过程中，仍然可能有新的key过来，为了清理的时候一并清理，
     *                     不漏，这里在put key之前做这个操作，形成映射
     * @throws Throwable
     */
    //如果没准备好，会阻塞
    void waitIfNotReady(MessageQueue messageQueue) throws Throwable;


    byte[] get(byte[] key) throws Throwable;
//    <K, V> V get(K key) throws Throwable;

//    <K, V> void put(MessageQueue messageQueue, K key, V value) throws Throwable;

    void put(MessageQueue stateTopicMessageQueue, byte[] key, byte[] value) throws Throwable;
    //只能查询到 < keyPrefix的结果，不包含等于
    <V> List<Pair<String, V>> searchLessThanKeyPrefix(String keyObject, long watermark, TypeReference<V> valueTypeRef) throws Throwable;

    List<Pair<byte[], byte[]>> searchStateLessThanWatermark(String operatorName, long lessThanThisTime, ValueMapperAction<byte[], WindowKey> deserializer) throws Throwable;

    <V> List<Pair<String, V>> searchMatchKeyPrefix(String keyPrefix, TypeReference<V> valueTypeRef) throws Throwable;

//    <K> void delete(K key) throws Throwable;

    void delete(byte[] key) throws Throwable;

    void persist(Set<MessageQueue> messageQueue) throws Throwable;
}
