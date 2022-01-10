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

import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.state.impl.JoinState;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

import java.util.List;

public interface IStorage {
    String SEPARATOR = "@";

    void putWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType, List<WindowBaseValue> windowBaseValue);

    List<WindowBaseValue> getWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    //用windowInstanceId删除所有WindowBaseValue【包括WindowValue、JoinState】
    void deleteWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    //各种窗口类型的最大分区编号
    void putMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType, long maxPartitionNum);

    Long getMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    void deleteMaxPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    void putMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType, long minPartitionNum);

    void getMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);

    void deleteMinPartitionNum(String windowInstanceId, WindowType windowType, WindowJoinType joinType);
}
