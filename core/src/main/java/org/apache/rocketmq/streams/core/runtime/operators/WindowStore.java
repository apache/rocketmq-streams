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


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.util.ArrayList;
import java.util.List;

public class WindowStore {
    private StateStore stateStore;

    private RocksDB rocksDB;


    public WindowStore(StateStore stateStore) {
        this.stateStore = stateStore;
        this.rocksDB = this.stateStore.getRocksDBStore().getRocksDB();
    }

    public <K,V> void put(MessageQueue stateTopicMessageQueue, String key, WindowState<K, V> value) {
        byte[] bytes = Utils.object2Byte(value);
        this.stateStore.put(stateTopicMessageQueue, key, bytes);
    }

    public <K, V> WindowState<K, V> get(String key) {
        return this.stateStore.get(key);
    }


    @SuppressWarnings("unchecked")
    public <K, V> List<Pair<String, WindowState<K, V>>> searchByKeyPrefix(String keyPrefix) {
        RocksIterator rocksIterator = rocksDB.newIterator();

        byte[] keyPrefixBytes = Utils.object2Byte(keyPrefix);
        rocksIterator.seekForPrev(keyPrefixBytes);

        List<Pair<String, WindowState<K, V>>> temp = new ArrayList<>();
        while (rocksIterator.isValid()) {
            byte[] keyBytes = rocksIterator.key();
            byte[] valueBytes = rocksIterator.value();

            String key = Utils.byte2Object(keyBytes, String.class);
            WindowState<K, V> value = Utils.byte2Object(valueBytes, WindowState.class);
            temp.add(new Pair<>(key, value));

            rocksIterator.prev();
        }

        return temp;
    }


    public void deleteByKey(String key) throws Throwable {
        this.stateStore.delete(key);
    }

}
