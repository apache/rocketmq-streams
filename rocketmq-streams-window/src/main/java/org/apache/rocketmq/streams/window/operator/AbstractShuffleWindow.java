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
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.IMessage;
import org.apache.rocketmq.streams.window.model.WindowInstance;
import org.apache.rocketmq.streams.window.shuffle.ShuffleChannel;
import org.apache.rocketmq.streams.window.storage.WindowStorage;
import org.apache.rocketmq.streams.window.storage.rocketmq.StorageDelegator;
import org.apache.rocketmq.streams.window.trigger.WindowTrigger;

public abstract class AbstractShuffleWindow extends AbstractWindow {

    protected transient ShuffleChannel shuffleChannel;
    protected transient AtomicBoolean hasCreated = new AtomicBoolean(false);

    @Override
    protected boolean initConfigurable() {
        //todo 为什么需要由window来实例，storage应该不属于某个window
        storage = new StorageDelegator(isLocalStorageOnly);
        return super.initConfigurable();
    }

    @Override
    public void windowInit() {
        if (hasCreated.compareAndSet(false, true)) {
            this.windowFireSource = new WindowTrigger(this);
            this.windowFireSource.init();
            this.windowFireSource.start(getFireReceiver());
            this.shuffleChannel = new ShuffleChannel(this);
            this.shuffleChannel.init();
            windowCache.setBatchSize(5000);
            windowCache.setShuffleChannel(shuffleChannel);
        }
    }

    @Override
    public AbstractContext<IMessage> doMessage(IMessage message, AbstractContext context) {
        shuffleChannel.startChannel();
        return super.doMessage(message, context);
    }

    @Override
    public int fireWindowInstance(WindowInstance windowInstance) {
        Set<String> splitIds = new HashSet<>();
        splitIds.add(windowInstance.getSplitId());
        shuffleChannel.flush(splitIds);

        return doFireWindowInstance(windowInstance);
    }

    /**
     * 接收shuffle后的消息进行计算，子类实现具体计算逻辑
     *
     * @param messages
     * @param instance
     */
    public abstract void shuffleCalculate(List<IMessage> messages, WindowInstance instance, String queueId);

    /**
     * 触发window
     *
     * @param instance
     */
    protected abstract int doFireWindowInstance(WindowInstance instance);

    public abstract void clearCache(String queueId);
}
