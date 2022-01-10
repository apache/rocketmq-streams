package org.apache.rocketmq.streams.window.storage.rocketmq;
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

import org.apache.rocketmq.streams.state.kv.rocksdb.RocksDBOperator;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.rocksdb.RocksDB;

import java.util.List;

public class LocalStorage implements IStorage {
    private static String UTF8 = "UTF8";
    private static RocksDB rocksDB = new RocksDBOperator().getInstance();


    @Override
    public void putWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType, List<WindowBaseValue> windowBaseValue) {

    }

    @Override
    public List<WindowBaseValue> getWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return null;
    }

    @Override
    public void deleteWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }

    @Override
    public void putMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType, long maxPartitionNum) {

    }

    @Override
    public Long getMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return null;
    }

    @Override
    public void deleteMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }

    @Override
    public void putMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType, long minPartitionNum) {

    }

    @Override
    public void getMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }

    @Override
    public void deleteMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }
}
