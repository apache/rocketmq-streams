package org.apache.rocketmq.streams.window.storage;
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

import org.apache.rocketmq.streams.common.utils.SerializeUtil;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class RocksdbIterator<T> implements Iterator<IteratorWrap<T>> {
    private String keyPrefix;
    private ReadOptions readOptions = new ReadOptions();
    private RocksIterator rocksIterator;


    public RocksdbIterator() {
    }

    public RocksdbIterator(String keyPrefix, RocksDB rocksDB) {
        this.keyPrefix = keyPrefix;
        this.rocksIterator = rocksDB.newIterator(readOptions);
        this.rocksIterator.seek(keyPrefix.getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = rocksIterator.isValid();
        String key = new String(rocksIterator.key());

        if (!key.startsWith(keyPrefix)) {
            hasNext = false;
        }
        return hasNext;
    }

    @Override
    public IteratorWrap<T> next() {
        String key = new String(rocksIterator.key());

        T data = SerializeUtil.deserialize(rocksIterator.value());
        IteratorWrap<T> result = new IteratorWrap<>(key, data, rocksIterator.value());

        rocksIterator.next();
        return result;
    }
}


