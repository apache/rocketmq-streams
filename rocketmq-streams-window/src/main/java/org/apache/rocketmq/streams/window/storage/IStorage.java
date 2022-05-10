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

import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

public interface IStorage {
    String SEPARATOR = "@";

    Future<?> load(Set<String> shuffleIds);


    void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance);

    <T> RocksdbIterator<T> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName);

    /**
     * WindowInstance的唯一索引字段
     *
     * @param windowInstanceId
     */
    void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceId);

    void putWindowBaseValue(String shuffleId, String windowInstanceId,
                            WindowType windowType, WindowJoinType joinType,
                            List<WindowBaseValue> windowBaseValue);

    void putWindowBaseValueIterator(String shuffleId, String windowInstanceId,
                                    WindowType windowType, WindowJoinType joinType,
                                    RocksdbIterator<? extends WindowBaseValue> windowBaseValueIterator);

    <T> RocksdbIterator<T> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType);


    //用windowInstanceId删除所有WindowBaseValue【包括WindowValue、JoinState】
    void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType, String msgKey);

    String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId);

    void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset);

    void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId);


    void putMaxPartitionNum(String shuffleId, String windowInstanceId, long maxPartitionNum);

    Long getMaxPartitionNum(String shuffleId, String windowInstanceId);

    void deleteMaxPartitionNum(String shuffleId, String windowInstanceId);

    int flush(List<String> queueId);

    void clearCache(String queueId);
}
