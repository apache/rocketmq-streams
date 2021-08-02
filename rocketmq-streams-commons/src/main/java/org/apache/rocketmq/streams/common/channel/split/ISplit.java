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
package org.apache.rocketmq.streams.common.channel.split;

import org.apache.rocketmq.streams.common.datatype.IJsonable;

import java.io.Serializable;

/**
 * 对消息队列分片的抽象。代表一个分片
 *
 * @param <T>
 * @param <Q>
 */
public interface ISplit<T, Q> extends Comparable<T>, Serializable, IJsonable {

    String getQueueId();

    /**
     * 比当前queueId大的queueId的值，最好只大一点点，主要应用在rocksdb 做范围查询,因为结束部分是开区间，只要大一点点就行
     *
     * @return
     */
    String getPlusQueueId();

    /**
     * 获取具体的队列 获取具体的队列
     *
     * @return
     */
    Q getQueue();

    //    public T getQueue();

}
