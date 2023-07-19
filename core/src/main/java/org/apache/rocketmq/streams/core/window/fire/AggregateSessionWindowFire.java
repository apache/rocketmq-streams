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
import org.apache.rocketmq.streams.core.exception.RStreamsException;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.window.WindowState;
import org.apache.rocketmq.streams.core.window.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.BiFunction;

public class AggregateSessionWindowFire<K, V, OV> extends AbstractWindowFire<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AggregateWindowFire.class);

    private final WindowStore<K, OV> windowStore;

    public AggregateSessionWindowFire(WindowStore<K, OV> windowStore,
                                      MessageQueue stateTopicMessageQueue,
                                      StreamContext<V> context,
                                      BiFunction<Long, MessageQueue, Long> commitWatermark) {
        super(context, stateTopicMessageQueue, commitWatermark);
        this.windowStore = windowStore;
    }

    @Override
    public List<WindowKey> fire(String operatorName, long watermark) {
        List<WindowKey> fired = new ArrayList<>();

        try {
            List<Pair<WindowKey, WindowState<K, OV>>> pairs = this.windowStore.searchLessThanWatermark(operatorName, watermark);

            Iterator<Pair<WindowKey, WindowState<K, OV>>> iterator = pairs.iterator();
            while (iterator.hasNext()) {
                Pair<WindowKey, WindowState<K, OV>> pair = iterator.next();
                WindowKey windowKey = pair.getKey();
                WindowState<K, OV> state = pair.getValue();

                long windowEnd = windowKey.getWindowEnd();
                long windowBegin;
                if (state.getRecordEarliestTimestamp() == Long.MAX_VALUE) {
                    windowBegin = windowKey.getWindowStart();
                } else {
                    windowBegin = state.getRecordEarliestTimestamp();
                }

                logger.info("fire session,windowKey={}, search keyPrefix={}, window: [{} - {}]", windowKey,
                        state.getKey().toString(), Utils.format(windowBegin), Utils.format(windowEnd));

                Properties header = this.context.getHeader();
                header.put(Constant.WINDOW_START_TIME, windowBegin);
                header.put(Constant.WINDOW_END_TIME, windowEnd);

                Data<K, OV> result = new Data<>(state.getKey(), state.getValue(), state.getRecordLastTimestamp(), header);
                Data<K, V> convert = this.convert(result);

                this.context.forward(convert);

                //删除状态
                this.windowStore.deleteByKey(windowKey);

                fired.add(windowKey);
            }

            return fired;
        } catch (Throwable t) {
            String format = String.format("fire session window error, name:%s", operatorName);
            throw new RStreamsException(format, t);
        }
    }

}
