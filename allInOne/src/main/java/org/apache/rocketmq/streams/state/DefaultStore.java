package org.apache.rocketmq.streams.state;
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

public class DefaultStore<K,V> extends AbstractStore<K,V> {
    protected StoreState state = StoreState.UNINITIALIZED;
    protected final Object lock = new Object();
    private RocksDBStore<K,V> rocksDBStore;

    public DefaultStore(RocksDBStore<K,V> rocksDBStore) {
        this.rocksDBStore = rocksDBStore;
    }

    @Override
    public void init() {
        synchronized (lock) {
            if (state == StoreState.UNINITIALIZED) {
                synchronized (lock) {
                    this.rocksDBStore.init();
                    state = StoreState.INITIALIZED;
                }
            }
        }
    }

    @Override
    public void recover() {

    }

    @Override
    public V get(K v) {
        return null;
    }

    @Override
    public void put(K k, V v) {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
