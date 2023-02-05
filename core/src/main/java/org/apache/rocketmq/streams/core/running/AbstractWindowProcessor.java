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
package org.apache.rocketmq.streams.core.running;


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.exception.RStreamsException;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.core.window.fire.IdleWindowScaner;
import org.apache.rocketmq.streams.core.window.Window;
import org.apache.rocketmq.streams.core.window.WindowInfo;
import org.apache.rocketmq.streams.core.window.fire.AccumulatorWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AccumulatorSessionWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AggregateSessionWindowFire;
import org.apache.rocketmq.streams.core.window.fire.AggregateWindowFire;
import org.apache.rocketmq.streams.core.window.fire.JoinWindowFire;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractWindowProcessor<V> extends AbstractProcessor<V> {
    protected IdleWindowScaner idleWindowScaner;
    protected AccumulatorWindowFire<?, ?, ?, ?> accumulatorWindowFire;
    protected AccumulatorSessionWindowFire<?, ?, ?, ?> accumulatorSessionWindowFire;

    protected AggregateWindowFire<?, ?, ?> aggregateWindowFire;
    protected AggregateSessionWindowFire<?, ?, ?> aggregateSessionWindowFire;

    protected JoinWindowFire<?, ?, ?, ?> joinWindowFire;

    protected List<Window> calculateWindow(WindowInfo windowInfo, long valueTime) {
        long sizeInterval = windowInfo.getWindowSize().toMillSecond();
        long slideInterval = windowInfo.getWindowSlide().toMillSecond();

        List<Window> result = new ArrayList<>((int) (sizeInterval / slideInterval));
        long lastStart = valueTime - (valueTime + slideInterval) % slideInterval;

        for (long start = lastStart; start > valueTime - sizeInterval; start -= slideInterval) {
            long end = start + sizeInterval;
            Window window = new Window(start, end);
            result.add(window);
        }
        return result;
    }


    protected long watermark(long watermark, MessageQueue stateTopicMessageQueue) {
        try {
            StateStore stateStore = this.context.getStateStore();

            byte[] watermarkBytes = stateStore.get(Constant.WATERMARK_KEY);
            long oldWatermark = Utils.bytes2Long(watermarkBytes);

            if (watermark > oldWatermark) {
                byte[] newWatermarkBytes = Utils.long2Bytes(watermark);
                stateStore.put(stateTopicMessageQueue, Constant.WATERMARK_KEY, newWatermarkBytes);
            } else {
                watermark = oldWatermark;
            }
        } catch (Throwable t) {
            throw new RStreamsException(t);
        }


        return watermark;
    }

}
