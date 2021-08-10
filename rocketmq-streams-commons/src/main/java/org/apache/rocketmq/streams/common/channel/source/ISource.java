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
package org.apache.rocketmq.streams.common.channel.source;

import java.util.Set;

import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;

public interface ISource<T extends ISource> extends IConfigurable, IStageBuilder<T> {
    String TYPE = "source";

    /**
     * 开始接收数据，把收到的数据交给receiver处理
     *
     * @param receiver 处理流数据
     * @return 是否正常启动
     */
    boolean start(IStreamOperator receiver);

    /**
     * 同一个group name共同消费一份数据，主要针对消息队列，如果实现的子类用到这个字段，需要保持语义
     *
     * @return 组名
     */
    String getGroupName();

    /**
     * 设置组名
     *
     * @param groupName 组名
     */
    void setGroupName(String groupName);

    /**
     * 需要的最大处理线程
     *
     * @return 最大处理线程
     */
    int getMaxThread();

    /**
     * 设置线程
     *
     * @param maxThread 线程数
     */
    void setMaxThread(int maxThread);

    /**
     * 每次最大抓取个数，只针对消息队列适用，这个参数主要可以控制内存占用
     *
     * @param size 每次最大抓取个数
     */
    void setMaxFetchLogGroupSize(int size);

    /**
     * 消息超过多长时间，会被checkpoint一次，对于批量消息无效
     *
     * @return checkpoint时间
     */
    long getCheckpointTime();

}
