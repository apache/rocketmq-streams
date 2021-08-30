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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.rocketmq.streams.common.configurable.IAfterConfigurableRefreshListener;
import org.apache.rocketmq.streams.common.configurable.IConfigurableService;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.shuffle.ShuffleChannel;
import org.apache.rocketmq.streams.window.source.WindowRireSource;
import org.apache.rocketmq.streams.window.state.WindowBaseValue;
import org.apache.rocketmq.streams.window.storage.WindowStorage;

public abstract class AbstractShuffleWindow extends AbstractWindow implements IAfterConfiguableRefreshListerner {

    protected transient ShuffleChannel shuffleChannel;
    protected transient AtomicBoolean hasCreated = new AtomicBoolean(false);

    @Override
    protected boolean initConfigurable() {
        storage = new WindowStorage();
        storage.setLocalStorageOnly(isLocalStorageOnly);
        return super.initConfigurable();
    }

    @Override
    public void doProcessAfterRefreshConfigurable(IConfigurableService configurableService) {
        if (hasCreated.compareAndSet(false, true)) {
            this.windowFireSource = new WindowRireSource(this);
            this.windowFireSource.init();
            this.windowFireSource.start(getFireReceiver());
            this.shuffleChannel = new ShuffleChannel(this);
            windowCache.setBatchSize(5000);
            windowCache.setShuffleChannel(shuffleChannel);
            shuffleChannel.startChannel();
        }
    }

    @Override
    public int fireWindowInstance(WindowInstance windowInstance, Map<String, String> queueId2Offset) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(windowInstance.getSplitId());
        shuffleChannel.flush(splitIds);
        int fireCount = fireWindowInstance(windowInstance, windowInstance.getSplitId(), queueId2Offset);
        return fireCount;
    }

    /**
     * 接收shuffle后的消息进行计算，子类实现具体计算逻辑
     *
     * @param messages
     * @param instance
     */
    public abstract void  shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId);
     /**
     * 触发window
     *
     * @param instance
     */
    protected abstract int fireWindowInstance(WindowInstance instance, String queueId, Map<String, String> queueId2Offset);

    public abstract void clearCache(String queueId);
}
