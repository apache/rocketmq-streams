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


import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.rocksdb.RocksDB;

import java.util.List;

public class WindowStore {
    private StateStore stateStore;

    private RocksDB rocksDB;


    public WindowStore(StateStore stateStore) {
        this.stateStore = stateStore;
        this.rocksDB = this.stateStore.getRocksDBStore().getRocksDB();
    }

    public <K, V> void put(MessageQueue stateTopicMessageQueue, String key, WindowState<K, V> value) throws Throwable {
        this.stateStore.put(stateTopicMessageQueue, key, value);
    }

    public <K, V> WindowState<K, V> get(String key) throws Throwable {
        return this.stateStore.get(key);
    }

    public <V> List<Pair<String, V>> searchLessThanKeyPrefix(String keyPrefix, TypeReference<V> valueTypeRef) throws Throwable {
        return this.stateStore.searchLessThanKeyPrefix(keyPrefix, valueTypeRef);
    }

    public <V> List<Pair<String, V>> searchMatchKeyPrefix(String keyPrefix, TypeReference<V> valueTypeRef) throws Throwable {
        return this.stateStore.searchMatchKeyPrefix(keyPrefix, valueTypeRef);
    }

    public void deleteByKey(String key) throws Throwable {
        if (StringUtils.isEmpty(key)) {
            return;
        }
        this.stateStore.delete(key);
    }

}
