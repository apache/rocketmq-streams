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
package org.apache.rocketmq.streams.common.channel.source.systemmsg;

import java.util.Set;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;

/**
 * 系统消息，当数据源分片减少时发送消息，主要应用在窗口计算，窗口清理这个分片的状态，加载新分片状态，不会跨shuffle
 */
public class RemoveSplitMessage extends SplitChangedMessage {

    public RemoveSplitMessage(Set<String> splitIds, Set<String> currentSplitIds) {
        super(splitIds, currentSplitIds, true);
    }

    @Override public int getSystemMessageType() {
        return ISystemMessage.SPLIT_REMOVE;
    }
}