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
package org.apache.rocketmq.streams.common.channel.sinkcache;

import java.util.Set;

/**
 * 消息缓存组件，支持消息缓存，刷新逻辑
 *
 * @param <R>
 */
public interface IMessageCache<R> {

    /**
     * 把消息插入缓存中
     *
     * @param msg 待缓存的消息
     * @return
     */
    int addCache(R msg);

    /**
     * 把队列排空，并写入到存储中
     *
     * @return
     */
    int flush();

    /**
     * 刷新某个分片
     *
     * @return
     */
    int flush(Set<String> splitId);

    /**
     * 返回消息个数
     *
     * @return
     */
    Integer getMessageCount();

    /**
     * 调用这个方法后，不必调用flush，由框架定时或定批完成刷新
     */
    void openAutoFlush();

    void closeAutoFlush();

    /**
     * 获取批次大小
     *
     * @return
     */
    int getBatchSize();

    /**
     * 设置缓存大小，超过条数，强制刷新
     *
     * @param batchSize
     */
    void setBatchSize(int batchSize);
}
