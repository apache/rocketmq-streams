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
package org.apache.rocketmq.streams.core.window.fire;


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.HashSet;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class AbstractWindowFire<K, V> implements WindowFire<K, V> {
    protected final StreamContext<V> context;
    private final MessageQueue stateTopicMessageQueue;
    private final BiFunction<Long, MessageQueue, Long> commitWatermark;

    public AbstractWindowFire(StreamContext<V> context,
                              MessageQueue stateTopicMessageQueue,
                              BiFunction<Long, MessageQueue, Long> commitWatermark) {
        this.context = context;
        this.stateTopicMessageQueue = stateTopicMessageQueue;
        this.commitWatermark = commitWatermark;
    }

    void commitWatermark(long watermark) {
        this.commitWatermark.apply(watermark, stateTopicMessageQueue);
    }

}
