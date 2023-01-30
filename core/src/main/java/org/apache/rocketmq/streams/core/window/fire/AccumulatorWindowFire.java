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

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.exception.RStreamsException;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.window.WindowState;
import org.apache.rocketmq.streams.core.window.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class AccumulatorWindowFire<K, R, V, OV> implements WindowFire<K, V> {
    private static final Logger logger = LoggerFactory.getLogger(AccumulatorWindowFire.class);

    private final WindowStore<K, Accumulator<R, OV>> windowStore;
    private final StreamContext<V> context;
    private final Consumer<WindowKey> consumer;

    public AccumulatorWindowFire(WindowStore<K, Accumulator<R, OV>> windowStore,
                                 StreamContext<V> context,
                                 Consumer<WindowKey> consumer) {
        this.windowStore = windowStore;
        this.context = context;
        this.consumer = consumer;
    }


    public void fire(String operatorName, long watermark) {
        try {
            List<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> pairs = windowStore.searchLessThanWatermark(operatorName, watermark);

            //pairs中最后一个时间最小，应该最先触发
            for (int i = pairs.size() - 1; i >= 0; i--) {
                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = pairs.get(i);

                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> value = pair.getValue();

                Long windowEnd = windowKey.getWindowEnd();

                Properties header = context.getHeader();
                header.put(Constant.WINDOW_START_TIME, windowKey.getWindowStart());
                header.put(Constant.WINDOW_END_TIME, windowEnd);

                Accumulator<R, OV> rovAccumulator = value.getValue();
                OV data = rovAccumulator.result(header);

                Data<K, OV> result = new Data<>(value.getKey(), data, value.getRecordLastTimestamp(), header);
                Data<K, V> convert = this.convert(result);

                if (logger.isDebugEnabled()) {
                    logger.debug("fire window, windowKey={}, search watermark={}, window: [{} - {}], data to next:[{}]", windowKey,
                            watermark, Utils.format(windowKey.getWindowStart()), Utils.format(windowEnd), convert);
                }

                context.forward(convert);

                //删除状态
                windowStore.deleteByKey(windowKey);
                this.consumer.accept(windowKey);
            }
        } catch (Throwable t) {
            String format = String.format("fire window error, watermark:%s, operatorName:%s", watermark, operatorName);
            throw new RStreamsException(format, t);
        }

    }



}
