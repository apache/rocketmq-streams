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

public class StorageDelegator implements IStorage {
    private MemoryStorage memoryStorage;
    private RemoteStorage remoteStorage;
    private boolean isLocalStorageOnly;

    public StorageDelegator(boolean isLocalStorageOnly) {
        this.memoryStorage = new MemoryStorage();
        this.remoteStorage = new RemoteStorage();
        this.isLocalStorageOnly = isLocalStorageOnly;
    }


    @Override
    public void init() {

    }

    @Override
    public void start() {

    }

    @Override
    public void putWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, WindowInstance windowInstance) {
        this.memoryStorage.putWindowInstance(shuffleId, windowNamespace, windowConfigureName, windowInstance);
    }

    @Override
    public List<WindowInstance> getWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName) {
        return this.memoryStorage.getWindowInstance(shuffleId, windowNamespace, windowConfigureName);
    }

    @Override
    public void deleteWindowInstance(String shuffleId, String windowNamespace, String windowConfigureName, String windowInstanceKey) {
        this.memoryStorage.deleteWindowInstance(shuffleId, windowNamespace, windowConfigureName, windowInstanceKey);
    }

    @Override
    public void putWindowBaseValue(String shuffleId, String windowInstanceId,
                                   WindowType windowType, WindowJoinType joinType,
                                   List<WindowBaseValue> windowBaseValue) {
        this.memoryStorage.putWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType, windowBaseValue);
    }

    @Override
    public List<WindowBaseValue> getWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return this.memoryStorage.getWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType);
    }

    @Override
    public void deleteWindowBaseValue(String shuffleId, String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        this.memoryStorage.deleteWindowBaseValue(shuffleId, windowInstanceId, windowType, joinType);
    }

    @Override
    public String getMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        return this.memoryStorage.getMaxOffset(shuffleId, windowConfigureName, oriQueueId);
    }

    @Override
    public void putMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId, String offset) {
        this.memoryStorage.putMaxOffset(shuffleId, windowConfigureName, oriQueueId, offset);
    }

    @Override
    public void deleteMaxOffset(String shuffleId, String windowConfigureName, String oriQueueId) {
        this.memoryStorage.deleteMaxOffset(shuffleId, windowConfigureName, oriQueueId);
    }

    @Override
    public void putMaxPartitionNum(String shuffleId, String windowInstanceKey, long maxPartitionNum) {
        this.memoryStorage.putMaxPartitionNum(shuffleId, windowInstanceKey, maxPartitionNum);
    }

    @Override
    public Long getMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        return this.memoryStorage.getMaxPartitionNum(shuffleId, windowInstanceKey);
    }

    @Override
    public void deleteMaxPartitionNum(String shuffleId, String windowInstanceKey) {
        this.memoryStorage.deleteMaxPartitionNum(shuffleId, windowInstanceKey);
    }

    @Override
    public int flush(List<String> queueId) {
        return this.memoryStorage.flush(queueId);
    }

    @Override
    public void clearCache(String queueId) {
        this.memoryStorage.clearCache(queueId);
    }

}
