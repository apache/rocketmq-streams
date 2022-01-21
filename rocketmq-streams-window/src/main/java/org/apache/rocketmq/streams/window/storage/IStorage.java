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

public interface IStorage {
    String SEPARATOR = "@";

    void init();

    void start();

    void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance);

    List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName);

    /**
     * WindowInstance的唯一索引字段
     *
     * @param windowInstanceKey
     */
    void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey);

    void putWindowBaseValue(String shuffleId, String windowInstanceId,
                            WindowType windowType, WindowJoinType joinType,
                            List<WindowBaseValue> windowBaseValue);


    List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType);


    //用windowInstanceId删除所有WindowBaseValue【包括WindowValue、JoinState】
    void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType);


    String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId);

    void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset);

    void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId);


    void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum);

    Long getMaxPartitionNum(String shuffleId, String windowInstanceKey);

    void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey);

    int flush(List<String> queueId);

    void clearCache(String queueId);

    //各种窗口类型的最大分区编号
//    void deleteMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);
//
//    void putMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType, long minPartitionNum);
//
//    Long getMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);
//
//    void deleteMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);
}
