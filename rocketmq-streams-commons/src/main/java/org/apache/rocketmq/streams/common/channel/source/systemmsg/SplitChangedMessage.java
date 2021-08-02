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

import org.apache.rocketmq.streams.common.channel.source.ISource;
import org.apache.rocketmq.streams.common.interfaces.IStreamOperator;
import org.apache.rocketmq.streams.common.interfaces.ISystemMessage;

public class SplitChangedMessage implements ISystemMessage {
    protected Set<String> splitIds;
    protected boolean needFlush;//需要同步刷新
    protected ISource source;//数据源对象
    protected IStreamOperator streamOperator;//当前的pipline
    protected Set<String> currentSplitIds = null;

    public SplitChangedMessage(Set<String> splitIds, Set<String> currentSplitIds, boolean needFlush) {
        this.splitIds = splitIds;
        this.needFlush = needFlush;
        this.currentSplitIds = currentSplitIds;
    }

    public Set<String> getSplitIds() {
        return splitIds;
    }

    public void setSplitIds(Set<String> splitIds) {
        this.splitIds = splitIds;
    }

    public boolean isNeedFlush() {
        return needFlush;
    }

    public void setNeedFlush(boolean needFlush) {
        this.needFlush = needFlush;
    }

    public ISource getSource() {
        return source;
    }

    public void setSource(ISource source) {
        this.source = source;
    }

    public IStreamOperator getStreamOperator() {
        return streamOperator;
    }

    public void setStreamOperator(IStreamOperator streamOperator) {
        this.streamOperator = streamOperator;
    }

    public Set<String> getCurrentSplitIds() {
        return currentSplitIds;
    }
}
