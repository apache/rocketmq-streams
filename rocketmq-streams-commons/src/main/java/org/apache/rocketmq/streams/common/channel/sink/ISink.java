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
package org.apache.rocketmq.streams.common.channel.sink;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.channel.sinkcache.IMessageFlushCallBack;
import org.apache.rocketmq.streams.common.channel.split.ISplit;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.configurable.IConfigurable;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.context.MessageOffset;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;
import org.apache.rocketmq.streams.common.topology.builder.IStageBuilder;


public interface ISink<T extends ISink> extends IConfigurable, IStageBuilder<T>, IMessageFlushCallBack<IMessage> {

    String TYPE = "sink";

    /**
     * 根据channel推断 meta，或者不需要meta，如消息对垒
     *
     * @param message
     * @return
     */
    boolean batchAdd(IMessage message, ISplit split);

    /**
     * 根据channel推断 meta，或者不需要meta，如消息对垒
     *
     * @param context
     * @return
     */
    boolean batchAdd(IMessage message);

    /**
     * 直接存储存储，不过缓存
     *
     * @param messages
     * @return
     */
    boolean batchSave(List<IMessage> messages);

    /**
     * 刷新某个分片
     *
     * @return
     */
    boolean flush(Set<String> splitId);


    /**
     * 刷新某个分片
     *
     * @return
     */
    boolean flush(String... splitIds);
    /**
     * 如果支持批量保存，此方法完成数据的全部写入
     *
     * @return
     */
    boolean flush();

    /**
     * 如果支持批量保存，此方法完成数据的全部写入
     *
     * @return
     */
    boolean checkpoint(Set<String> splitIds);



    /**
     * 如果支持批量保存，此方法完成数据的全部写入
     *
     * @return
     */
    boolean checkpoint(String... splitIds);
    /**
     * 调用这个方法后，不必调用flush，由框架定时或定批完成刷新
     */
    void openAutoFlush();

    void closeAutoFlush();

    /**
     * 设置缓存大小，超过条数，强制刷新
     *
     * @param batchSize
     */
    void setBatchSize(int batchSize);

    /**
     * 获取批次大小
     *
     * @return
     */
    int getBatchSize();

    Map<String, MessageOffset> getFinishedQueueIdAndOffsets(CheckPointMessage checkPointMessage);

    void atomicSink(ISystemMessage message);


}
