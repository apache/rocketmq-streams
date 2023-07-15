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
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.ColumnFamilyUtil;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.apache.rocketmq.streams.core.window.JoinType;
import org.apache.rocketmq.streams.core.window.StreamType;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.window.WindowState;
import org.apache.rocketmq.streams.core.window.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;

public class JoinWindowFire<K, V1, V2, OUT> {
    private static final Logger logger = LoggerFactory.getLogger(JoinWindowFire.class);

    private final JoinType joinType;
    private final MessageQueue stateTopicMessageQueue;
    private final StreamContext<Object> context;
    private final ValueJoinAction<V1, V2, OUT> joinAction;
    private final WindowStore<K, V1> leftWindowStore;
    private final WindowStore<K, V2> rightWindowStore;
    private final BiFunction<Long, MessageQueue, Long> commitWatermark;

    public JoinWindowFire(JoinType joinType,
                          MessageQueue stateTopicMessageQueue,
                          StreamContext<Object> context,
                          ValueJoinAction<V1, V2, OUT> joinAction,
                          WindowStore<K, V1> leftWindowStore,
                          WindowStore<K, V2> rightWindowStore,
                          BiFunction<Long, MessageQueue, Long> commitWatermark) {
        this.joinType = joinType;
        this.stateTopicMessageQueue = stateTopicMessageQueue;
        this.context = context;
        this.joinAction = joinAction;
        this.leftWindowStore = leftWindowStore;
        this.rightWindowStore = rightWindowStore;
        this.commitWatermark = commitWatermark;
    }

    public List<WindowKey> fire(String operatorName, long watermark, StreamType streamType) {
        List<WindowKey> fired = new ArrayList<>();

        try {
            String leftWindow = Utils.buildKey(operatorName, StreamType.LEFT_STREAM.name());
            List<Pair<WindowKey, WindowState<K, V1>>> leftPairs = this.leftWindowStore.searchLessThanWatermark(leftWindow, watermark);
            if (leftPairs.size() != 0) {
                for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                    logger.debug("search with key prefix:{} and watermark:{}, find window: {}", leftWindow, Utils.format(watermark), leftPair.getKey());
                }
            }

            String rightWindow = Utils.buildKey(operatorName, StreamType.RIGHT_STREAM.name());
            List<Pair<WindowKey, WindowState<K, V2>>> rightPairs = this.rightWindowStore.searchLessThanWatermark(rightWindow, watermark);
            if (rightPairs.size() != 0) {
                for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                    logger.debug("search with key prefix:{} and watermark:{}, find window: {}", rightWindow, Utils.format(watermark), rightPair.getKey());
                }
            }

            if (leftPairs.size() == 0 && rightPairs.size() == 0) {
                logger.debug("left window and right window are all empty, watermark:{}." +
                        "left window operatorName:{}, right window operatorName:{}", Utils.format(watermark), leftWindow, rightWindow);
                return fired;
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
                        WindowKey leftWindowKey = leftPair.getKey();
                        String leftPrefix = leftWindowKey.getKeyAndWindow();

                        for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                            String rightPrefix = rightPair.getKey().getKeyAndWindow();

                            //相同window中相同key，聚合
                            if (leftPrefix.equals(rightPrefix)) {
                                //do fire
                                V1 o1 = leftPair.getValue().getValue();
                                V2 o2 = rightPair.getValue().getValue();

                                OUT out = this.joinAction.apply(o1, o2);

                                Properties header = this.context.getHeader();
                                header.put(Constant.WINDOW_START_TIME, leftWindowKey.getWindowStart());
                                header.put(Constant.WINDOW_END_TIME, leftWindowKey.getWindowEnd());

                                assert leftPair.getValue().getKey() == rightPair.getValue().getKey();
                                Data<K, OUT> result = new Data<>(leftPair.getValue().getKey(), out, this.context.getDataTime(), header);
                                Data<K, Object> convert = this.convert(result);

                                this.context.forward(convert);

                                fired.add(leftWindowKey);
                            }
                        }
                    }
                    break;
                case LEFT_JOIN:
                    switch (streamType) {
                        case LEFT_STREAM:
                            //左流全部触发，不管右流匹配上没
                            for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                                WindowKey leftWindowKey = leftPair.getKey();

                                fired.add(leftWindowKey);

                                String leftPrefix = leftWindowKey.getKeyAndWindow();
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
                                    fired.add(targetPair.getKey());

                                    assert leftPair.getValue().getKey() == targetPair.getValue().getKey();
                                }

                                OUT out = this.joinAction.apply(o1, o2);
                                Properties header = this.context.getHeader();
                                header.put(Constant.WINDOW_START_TIME, leftWindowKey.getWindowStart());
                                header.put(Constant.WINDOW_END_TIME, leftWindowKey.getWindowEnd());


                                Data<K, OUT> result = new Data<>(leftPair.getValue().getKey(), out, this.context.getDataTime(), header);
                                Data<K, Object> convert = this.convert(result);

                                this.context.forward(convert);
                            }
                            break;
                        case RIGHT_STREAM:
                            //do nothing.
                    }
                    break;
            }

            if (leftPairs.size() != 0) {
                logger.debug("delete left window.");
                for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                    this.leftWindowStore.deleteByKey(leftPair.getKey());
                }
            }

            if (rightPairs.size() != 0) {
                logger.debug("delete right window.");
                for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                    this.rightWindowStore.deleteByKey(rightPair.getKey());
                }
            }
        } catch (Throwable t) {
            String format = String.format("fire window error, watermark:%s.", watermark);
            throw new RStreamsException(format, t);
        }

        return fired;
    }

    @SuppressWarnings("unchecked")
    private <K> Data<K, Object> convert(Data<?, ?> data) {
        return (Data<K, Object>) new Data<>(data.getKey(), data.getValue(), data.getTimestamp(), data.getHeader());
    }

    void commitWatermark(long watermark) throws Throwable {
        StateStore stateStore = this.context.getStateStore();

        //get old watermark
        byte[] keyBytes = Utils.watermarkKeyBytes(stateTopicMessageQueue, Constant.WATERMARK_KEY);
        byte[] watermarkBytes = stateStore.get(ColumnFamilyUtil.WATERMARK_STATE_CF, keyBytes);
        long oldWatermark = Utils.bytes2Long(watermarkBytes);

        if (watermark > oldWatermark) {
            this.commitWatermark.apply(watermark, stateTopicMessageQueue);
            Set<MessageQueue> set = new HashSet<>();
            set.add(stateTopicMessageQueue);
            stateStore.persist(set);
        }
    }
}
