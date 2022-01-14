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

import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

import java.util.List;

public interface IStorage {
    String SEPARATOR = "@";


    void putWindowInstance(String windowNamespace, String windowConfigureName, String shuffleId, WindowInstance windowInstance);

    List<WindowInstance> getWindowInstance(String windowNamespace, String windowConfigureName, String shuffleId);

    /**
     * WindowInstance的唯一索引字段
     *
     * @param windowInstanceKey
     */
    void deleteWindowInstance(String windowInstanceKey);

    void putWindowBaseValue(String windowInstanceId, String shuffleId,
                            WindowType windowType, WindowJoinType joinType,
                            List<WindowBaseValue> windowBaseValue);


    List<WindowBaseValue> getWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType);


    //用windowInstanceId删除所有WindowBaseValue【包括WindowValue、JoinState】
    void deleteWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType);


    String getMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId);

    void putMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId, String offset);

    void deleteMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId);


    void putMaxPartitionNum(String windowInstanceKey, String shuffleId, long maxPartitionNum);

    Long getMaxPartitionNum(String windowInstanceKey, String shuffleId);

    void deleteMaxPartitionNum(String windowInstanceKey, String shuffleId);

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
