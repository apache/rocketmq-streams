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

import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;

import java.util.List;

public class StorageDelegator implements IStorage {
    private LocalStorage localStorage;
    private RemoteStorage remoteStorage;
    private boolean isLocalStorageOnly;

    public StorageDelegator(boolean isLocalStorageOnly) {
        this.localStorage = new LocalStorage();
        this.remoteStorage = new RemoteStorage();
        this.isLocalStorageOnly = isLocalStorageOnly;
    }


    @Override
    public void putWindowInstance(String windowNamespace, String windowConfigureName, String shuffleId, WindowInstance windowInstance) {

    }

    @Override
    public List<WindowInstance> getWindowInstance(String windowNamespace, String windowConfigureName, String shuffleId) {
        return null;
    }

    @Override
    public void deleteWindowInstance(String windowInstanceKey) {

    }

    @Override
    public void putWindowBaseValue(String windowInstanceId, String shuffleId,
                                   WindowType windowType, WindowJoinType joinType,
                                   List<WindowBaseValue> windowBaseValue) {

    }

    @Override
    public List<WindowBaseValue> getWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {
        return null;
    }

    @Override
    public void deleteWindowBaseValue(String windowInstanceId, WindowType windowType, WindowJoinType joinType) {

    }

    @Override
    public String getMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId) {
        return null;
    }

    @Override
    public void putMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId, String offset) {

    }

    @Override
    public void deleteMaxOffset(String windowConfigureName, String shuffleId, String oriQueueId) {

    }

    @Override
    public void putMaxPartitionNum(String windowInstanceKey, String shuffleId, long maxPartitionNum) {

    }

    @Override
    public Long getMaxPartitionNum(String windowInstanceKey, String shuffleId) {
        return null;
    }

    @Override
    public void deleteMaxPartitionNum(String windowInstanceKey, String shuffleId) {

    }

    @Override
    public int flush(List<String> queueId) {
        return 0;
    }

    @Override
    public void clearCache(String queueId) {

    }

}
