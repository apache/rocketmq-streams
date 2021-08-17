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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.db.driver.batchloader.BatchRowLoader;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.apache.rocketmq.streams.db.driver.orm.ORMUtil;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;

public abstract class AbstractWindowStorage<T extends WindowBaseValue> implements IWindowStorage<T> {
    protected boolean isLocalStorageOnly = false;
    protected transient ExecutorService dataLoaderExecutor = new ThreadPoolExecutor(10, 10,
        0L, TimeUnit.MILLISECONDS,
        new LinkedBlockingQueue<Runnable>());
    ;

    @Override
    public Long getMaxShuffleId(String queueId, String windowNameSpace, String windowName, Class<T> clazz) {
        if (isLocalStorageOnly) {
            return null;
        }
        String sql = "select max(partition_num) as partition_num from " + ORMUtil.getTableName(clazz)
            + " where name_space ='" + windowNameSpace + "' and configure_name='" + windowName + "' and `partition`='" + queueId + "'";
        WindowBaseValue windowBaseValue = ORMUtil.queryForObject(sql, new HashMap<>(4), clazz);
        if (windowBaseValue == null) {
            return null;
        }
        return windowBaseValue.getPartitionNum();
    }

    @Override
    public void multiPut(Map<String, T> map, String windowInstanceId, String queueId) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public Map<String, T> multiGet(Class<T> clazz, List<String> keys, String windowInstanceId, String queueId) {
        throw new RuntimeException("can not support this method");
    }

    @Override
    public void loadSplitData2Local(String queueId, String windowInstanceId, Class<T> clazz, IRowOperator processor) {
        if (isLocalStorageOnly) {
            return;
        }
        String windowInstancePartitionId = StringUtil.createMD5Str(windowInstanceId);
        dataLoaderExecutor.execute(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                BatchRowLoader batchRowLoader = new BatchRowLoader("partition_num",
                    "select * from " + ORMUtil.getTableName(clazz) + "  where window_instance_partition_id ='"
                        + windowInstancePartitionId + "'", processor);
                batchRowLoader.startLoadData();
                ShufflePartitionManager.getInstance().setWindowInstanceFinished(windowInstanceId);
                System.out.println(System.currentTimeMillis() - start);
                System.out.println("");
            }
        });

    }

    @Override
    public void put(String key, T value) {
        Map<String, T> map = new HashMap<>();
        map.put(key, value);
        multiPut(map);
    }

    @Override
    public T get(Class<T> clazz, String key) {
        Map<String, T> result = multiGet(clazz, key);
        if (result == null) {
            return null;
        }
        return result.values().iterator().next();
    }

    @Override
    public Map<String, T> multiGet(Class<T> clazz, String... keys) {
        List<String> keyList = new ArrayList<>();
        for (String key : keys) {
            keyList.add(key);
        }
        return multiGet(clazz, keyList);
    }

    public boolean isLocalStorageOnly() {
        return isLocalStorageOnly;
    }

    public void setLocalStorageOnly(boolean localStorageOnly) {
        isLocalStorageOnly = localStorageOnly;
    }

}
