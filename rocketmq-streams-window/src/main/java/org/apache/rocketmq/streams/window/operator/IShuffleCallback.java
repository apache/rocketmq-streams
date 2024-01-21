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
package org.apache.rocketmq.streams.window.operator;

import java.util.List;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.common.topology.model.SectionPipeline;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.state.impl.WindowValue;

public interface IShuffleCallback {

    /**
     * 当收到消息时，应该如何计算，如count算子，没收到一条消息就会加1
     *
     * @param receiveMessages
     * @param instance
     */
    void accumulate(List<IMessage> receiveMessages, WindowInstance instance);

    /**
     * 当窗口触发时，应当如何处理
     *
     * @return
     */
    int fire(WindowInstance instance);

    /**
     * 窗口触发后清理数据
     *
     * @param windowInstance
     */
    void clearWindowInstance(WindowInstance windowInstance);

    /**
     * 发送数据到下一个节点
     *
     * @param windowValueList
     * @param queueId
     */
    void sendFireMessage(List<WindowValue> windowValueList, String queueId);

    /**
     * 窗口触发后，需要执行的逻辑
     *
     * @param receiver
     */
    void setFireReceiver(SectionPipeline receiver);

}
