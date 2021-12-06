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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ShufflePartitionManager {
    private static ShufflePartitionManager instance = new ShufflePartitionManager();
    protected Map<String, Boolean> splitId2AllWindowInstanceFinishInit = new HashMap<>();//split是否有效，这个分片下所有的window instacne都完成了初始化
    protected Map<String, Boolean> windowInstanceId2FinishInit = new HashMap<>();//window instance 是否完成初始化
    private ExecutorService executorService;

    private ShufflePartitionManager() {
        executorService = new ThreadPoolExecutor(10, 10,
            0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>());

    }

    public static ShufflePartitionManager getInstance() {
        return instance;
    }

    /**
     * if window instance finished init， return true else return false
     *
     * @param splitId
     * @param windowInstanceId
     * @return
     */
    public boolean isWindowInstanceFinishInit(String splitId, String windowInstanceId) {
        Boolean allSpliltFinish = splitId2AllWindowInstanceFinishInit.get(splitId);
        if (allSpliltFinish != null && allSpliltFinish) {
            return true;
        }
        Boolean windowInstanceInitFinished = windowInstanceId2FinishInit.get(windowInstanceId);
        if (windowInstanceInitFinished != null && windowInstanceInitFinished) {
            return true;
        }
        return false;
    }

    public synchronized void setSplitFinished(String splitId) {
        splitId2AllWindowInstanceFinishInit.put(splitId, true);
    }

    public synchronized void setSplitInValidate(String splitId) {
        splitId2AllWindowInstanceFinishInit.put(splitId, false);
    }

    public synchronized void setWindowInstanceFinished(String windowInstanceId) {
        windowInstanceId2FinishInit.put(windowInstanceId, true);
    }

    public synchronized void clearWindowInstance(String windowInstanceId) {
        windowInstanceId2FinishInit.remove(windowInstanceId);
    }

    public synchronized void clearSplit(String queueId) {
        splitId2AllWindowInstanceFinishInit.remove(queueId);
        Map<String, Boolean> map=new HashMap<>(this.windowInstanceId2FinishInit);
        for (String windowInstanceId : map.keySet()) {
            if (windowInstanceId.startsWith(queueId)) {
                this.windowInstanceId2FinishInit.remove(windowInstanceId);
            }
        }
    }
}
