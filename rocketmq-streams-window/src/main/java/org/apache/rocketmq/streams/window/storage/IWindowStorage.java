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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.db.driver.batchloader.IRowOperator;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage.WindowBaseValueIterator;

public interface IWindowStorage<T extends WindowBaseValue> extends ICommonStorage<T> {

    //多组key value批量存储
    void multiPut(Map<String, T> map, String windowInstanceId, String queueId);

    //获取多个key的值
    Map<String, T> multiGet(Class<T> clazz, List<String> keys, String windowInstanceId, String queueId);

    /***
     * 把queueId 前缀的数据全部失效掉
     * @param channelQueue 必须6位，64001  1280001 128分片总数，001第一个分片
     */

    void clearCache(ISplit channelQueue, Class<T> clazz);

    /**
     * 删除一个窗口实例的数据，包括远程和本地存储
     */
    void delete(String windowInstanceId, String queueId, Class<T> clazz);
    /**
     * 加载一个窗口实例的数据到本地存储
     */
    WindowBaseValueIterator<T> loadWindowInstanceSplitData(String localStorePrefix, String queueId, String windowInstanceId, String keyPrefix,
                                                           Class<T> clazz);

    /**
     * 这个窗口实例，最大的分片序列号，主要是通过db获取
     *
     * @return
     */
    Long getMaxSplitNum(WindowInstance windowInstance, Class<T> clazz);

    /**
     * 批量加载数据，放入本地缓存
     *
     * @param splitNumer
     * @param rowOperator
     */
    void loadSplitData2Local(String splitNumer, String windowInstanceId, Class<T> clazz, IRowOperator rowOperator);
}
