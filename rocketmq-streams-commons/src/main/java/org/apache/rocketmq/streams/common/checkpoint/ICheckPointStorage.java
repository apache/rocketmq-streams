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
package org.apache.rocketmq.streams.common.checkpoint;

import java.util.List;
import org.apache.rocketmq.streams.common.channel.source.ISource;

/**
 * @description 负责数据源offset的保存、恢复，一般用在AbstractPullSource上
 */
public interface ICheckPointStorage {

    String TYPE = "checkpoint_storage";

    /**
     * 选择
     *
     * @return
     */
    String getStorageName();

    /**
     * 保存分片和offset到状态存储中
     *
     * @param checkPointState
     * @param <T>
     */
    <T extends ISplitOffset> void save(List<T> checkPointState);

    /**
     * 给数据源恢复状态和存储
     *
     * @param iSource
     * @param queueId
     * @param <T>
     * @return
     */
    <T extends ISplitOffset> T recover(ISource<?> iSource, String queueId);

    /**
     * 刷新缓存的状态到存储中
     */
    void flush();

    /**
     * 把新收集的数据源分片和offset的信息保存到缓存
     *
     * @param message
     */
    void addCheckPointMessage(CheckPointMessage message);

    void finish();

}
