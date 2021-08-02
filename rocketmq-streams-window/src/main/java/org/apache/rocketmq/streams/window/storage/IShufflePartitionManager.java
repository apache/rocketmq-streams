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
package org.apache.rocketmq.streams.window.storage;

import org.apache.rocketmq.streams.common.channel.split.ISplit;

import java.util.Collection;

public interface IShufflePartitionManager {

    /**
     * 这个分片是否可用本地存储
     *
     * @param shuffleId
     * @return
     */
    boolean isLocalStorage(String shuffleId, String windowInstanceId);

    void setLocalStorageInvalid(ISplit channelQueue);

    void setLocalStorageInvalid(ISplit channelQueue, String windowInstanceId);

    /**
     * setLocalStorageInvalid 如果 shuffle id不存在，且
     *
     * @param shuffleId
     */
    boolean setLocalStorageValdateIfNotExist(String shuffleId, String windowInstanceId);

    /**
     * 当窗口实例触发后，通过这个方法，回收资源
     *
     * @param windowInstanceId
     * @param queueIds
     */
    void clearWindowInstanceStorageStatus(String windowInstanceId, Collection<String> queueIds);

}
