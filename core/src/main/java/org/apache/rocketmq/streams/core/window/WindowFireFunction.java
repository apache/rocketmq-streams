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
package org.apache.rocketmq.streams.core.window;

import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.exception.RStreamsException;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public class WindowFireFunction {
    private static final Logger logger = LoggerFactory.getLogger(WindowFireFunction.class.getName());

    public <K, R, V, OV> long fireNormalWindow(long watermark, String operatorName,
                                               WindowStore<K, Accumulator<R, OV>> windowStore,
                                               StreamContext<V> context,
                                               Function<Data<?, ?>, Data<K, V>> function) {
        try {
            List<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> pairs = windowStore.searchLessThanWatermark(operatorName, watermark);

            long maxWindowEnd = -1L;
            //pairs中最后一个时间最小，应该最先触发
            for (int i = pairs.size() - 1; i >= 0; i--) {
                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = pairs.get(i);

                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> value = pair.getValue();

                Long windowEnd = windowKey.getWindowEnd();
                if (windowEnd > maxWindowEnd) {
                    maxWindowEnd = windowEnd;
                }

                Properties header = context.getHeader();
                header.put(Constant.WINDOW_START_TIME, windowKey.getWindowStart());
                header.put(Constant.WINDOW_END_TIME, windowEnd);

                Accumulator<R, OV> rovAccumulator = value.getValue();
                OV data = rovAccumulator.result(header);

                Data<K, OV> result = new Data<>(value.getKey(), data, value.getRecordLastTimestamp(), header);
                Data<K, V> convert = function.apply(result);

                if (logger.isDebugEnabled()) {
                    logger.debug("fire window, windowKey={}, search watermark={}, window: [{} - {}], data to next:[{}]", windowKey,
                            watermark, Utils.format(windowKey.getWindowStart()), Utils.format(windowEnd), convert);
                }

                context.forward(convert);

                //删除状态
                windowStore.deleteByKey(windowKey);
            }

            return maxWindowEnd;
        } catch (Throwable t) {
            String format = String.format("fire window error, watermark:%s, operatorName:%s", watermark, operatorName);
            throw new RStreamsException(format, t);
        }

    }


    public <K, R, V, OV> void fireSessionWindow(String name,
                                                WindowStore<K, Accumulator<R, OV>> windowStore,
                                                StreamContext<V> context,
                                                Function<Data<?, ?>, Data<K, V>> function) {
        try {
            List<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> pairs = windowStore.searchMatchKeyPrefix(name);

            Iterator<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> iterator = pairs.iterator();
            while (iterator.hasNext()) {
                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = iterator.next();
                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> state = pair.getValue();

                long windowEnd = windowKey.getWindowEnd();
                long windowBegin;
                if (state.getRecordEarliestTimestamp() == Long.MAX_VALUE) {
                    windowBegin = windowKey.getWindowStart();
                } else {
                    windowBegin = state.getRecordEarliestTimestamp();
                }

                logger.info("fire session,windowKey={}, search keyPrefix={}, window: [{} - {}]",
                        windowKey, state.getKey().toString(), Utils.format(windowBegin), Utils.format(windowEnd));

                Properties header = context.getHeader();
                header.put(Constant.WINDOW_START_TIME, windowBegin);
                header.put(Constant.WINDOW_END_TIME, windowEnd);

                Accumulator<R, OV> value = state.getValue();
                OV data = value.result(header);

                Data<K, OV> result = new Data<>(state.getKey(), data, state.getRecordLastTimestamp(), header);
                Data<K, V> convert = function.apply(result);

                context.forward(convert);

                //删除状态
                windowStore.deleteByKey(windowKey);
            }
        } catch (Throwable t) {
            String format = String.format("fire session window error, name:%s", name);
            throw new RStreamsException(format, t);
        }
    }

    public <K, V1, V2, OUT> void fireJoinWindow(String name,
                                                long watermark,
                                                JoinType joinType,
                                                StreamType streamType,
                                                WindowStore<K, V1> leftWindowStore,
                                                WindowStore<K, V2> rightWindowStore,
                                                StreamContext<Object> context,
                                                ValueJoinAction<V1, V2, OUT> joinAction,
                                                Function<Data<?, ?>, Data<K, Object>> function) {
        try {
            String leftWindow = Utils.buildKey(name, StreamType.LEFT_STREAM.name());
            List<Pair<WindowKey, WindowState<K, V1>>> leftPairs = leftWindowStore.searchLessThanWatermark(leftWindow, watermark);

            String rightWindow = Utils.buildKey(name, StreamType.RIGHT_STREAM.name());
            List<Pair<WindowKey, WindowState<K, V2>>> rightPairs = rightWindowStore.searchLessThanWatermark(rightWindow, watermark);


            if (leftPairs.size() == 0 && rightPairs.size() == 0) {
                return;
            }

            leftPairs.sort(Comparator.comparing(pair -> {
                WindowKey key = pair.getKey();
                return key.getWindowEnd();
            }));
            rightPairs.sort(Comparator.comparing(pair -> {
                WindowKey key = pair.getKey();
                return key.getWindowEnd();
            }));

            switch (joinType) {
                case INNER_JOIN:
                    //匹配上才触发
                    for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                        String leftPrefix = leftPair.getKey().getKeyAndWindow();

                        for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                            String rightPrefix = rightPair.getKey().getKeyAndWindow();

                            //相同window中相同key，聚合
                            if (leftPrefix.equals(rightPrefix)) {
                                //do fire
                                V1 o1 = leftPair.getValue().getValue();
                                V2 o2 = rightPair.getValue().getValue();

                                OUT out = joinAction.apply(o1, o2);

                                Properties header = context.getHeader();
                                header.put(Constant.WINDOW_START_TIME, leftPair.getKey().getWindowStart());
                                header.put(Constant.WINDOW_END_TIME, leftPair.getKey().getWindowEnd());
                                Data<K, OUT> result = new Data<>(context.getKey(), out, context.getDataTime(), header);
                                Data<K, Object> convert = function.apply(result);
                                context.forward(convert);
                            }
                        }
                    }
                    break;
                case LEFT_JOIN:
                    switch (streamType) {
                        case LEFT_STREAM:
                            //左流全部触发，不管右流匹配上没
                            for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                                String leftPrefix = leftPair.getKey().getKeyAndWindow();
                                Pair<WindowKey, WindowState<K, V2>> targetPair = null;
                                for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                                    if (rightPair.getKey().getKeyAndWindow().equals(leftPrefix)) {
                                        targetPair = rightPair;
                                        break;
                                    }
                                }

                                //fire
                                V1 o1 = leftPair.getValue().getValue();
                                V2 o2 = null;
                                if (targetPair != null) {
                                    o2 = targetPair.getValue().getValue();
                                }

                                OUT out = joinAction.apply(o1, o2);
                                Properties header = context.getHeader();
                                header.put(Constant.WINDOW_START_TIME, leftPair.getKey().getWindowStart());
                                header.put(Constant.WINDOW_END_TIME, leftPair.getKey().getWindowEnd());
                                Data<K, OUT> result = new Data<>(context.getKey(), out, context.getDataTime(), header);
                                Data<K, Object> convert = function.apply(result);
                                context.forward(convert);
                            }
                            break;
                        case RIGHT_STREAM:
                            //do nothing.
                    }
                    break;
            }

            //删除状态
            for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                leftWindowStore.deleteByKey(leftPair.getKey());
            }

            for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                rightWindowStore.deleteByKey(rightPair.getKey());
            }
        } catch (Throwable t) {
            String format = String.format("fire window error, watermark:%s.", watermark);
            throw new RStreamsException(format, t);
        }

    }
}
