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
package org.apache.rocketmq.streams.common.interfaces;

import org.apache.rocketmq.streams.common.batchsystem.BatchFinishMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.NewSplitMessage;
import org.apache.rocketmq.streams.common.channel.source.systemmsg.RemoveSplitMessage;
import org.apache.rocketmq.streams.common.checkpoint.CheckPointMessage;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;

public interface ISystemMessageProcessor {

    /**
     * 当有新分片启动时，发系统消息，告诉所有算子，需要准备的，提前准备资源，比如窗口计算，在精确计算的场景，会把远程存储恢复到本地
     * 消息不会传递到shuffle侧，shuffle后端组件通过shuffle source通知
     *
     * @param message
     * @param context
     */
    void addNewSplit(IMessage message, AbstractContext context, NewSplitMessage newSplitMessage);

    /**
     * 当有分片移除时，发系统消息，告诉所有算子，刷新本地缓存。
     * 消息不会传递到shuffle侧，shuffle后端组件通过shuffle source通知
     *
     * @param message
     * @param context
     */
    void removeSplit(IMessage message, AbstractContext context, RemoveSplitMessage removeSplitMessage);

    /**
     * 当channel保存commit offset时，发送系统消息，收到系统消息的stage，需要刷新缓存。
     * 消息不会传递到shuffle侧，shuffle后端组件通过shuffle source通知
     *
     * @param message
     * @param context
     */
    void checkpoint(IMessage message, AbstractContext context, CheckPointMessage checkPointMessage);

    /**
     * 当是批量消息时，消息完成后，可以通过发送BatchFinishMessage消息，通知所有算子完成计算，此时窗口也会提前触发
     * 消息会传递到shuffle侧，通过shuffle所有组件都会收到
     *
     * @param message
     * @param context
     */
    void batchMessageFinish(IMessage message, AbstractContext context, BatchFinishMessage checkPointMessage);

    /**
     * 系统正式启动前，发通知给stage，需要提前初始化资源的stage，可以完成资源初始化，如dim，可以开始加载维表数据
     * 消息不会传递到shuffle侧，shuffle后端组件通过shuffle source通知
     */
    void startJob();

    /**
     * 系统正式关闭前，发通知给stage，完成资源回收
     * 消息不会传递到shuffle侧，shuffle后端组件通过shuffle source通知
     */
    void stopJob();
}
