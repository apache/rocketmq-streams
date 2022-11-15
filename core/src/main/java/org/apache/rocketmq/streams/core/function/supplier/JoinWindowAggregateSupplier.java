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
package org.apache.rocketmq.streams.core.function.supplier;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.running.AbstractWindowProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.runtime.operators.JoinType;
import org.apache.rocketmq.streams.core.runtime.operators.StreamType;
import org.apache.rocketmq.streams.core.runtime.operators.Window;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.runtime.operators.WindowKey;
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.runtime.operators.WindowStore;
import org.apache.rocketmq.streams.core.typeUtil.TypeExtractor;
import org.apache.rocketmq.streams.core.typeUtil.TypeWrapper;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import java.lang.reflect.Type;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

public class JoinWindowAggregateSupplier<K, V1, V2, OUT> implements Supplier<Processor<? super OUT>> {
    private static final Logger logger = LoggerFactory.getLogger(JoinWindowAggregateSupplier.class.getName());

    private String name;
    private WindowInfo windowInfo;
    private final ValueJoinAction<V1, V2, OUT> joinAction;
    private JoinType joinType;

    public JoinWindowAggregateSupplier(String name, WindowInfo windowInfo, ValueJoinAction<V1, V2, OUT> joinAction) {
        this.name = name;
        this.windowInfo = windowInfo;
        this.joinType = windowInfo.getJoinStream().getJoinType();
        this.joinAction = joinAction;
    }

    @Override
    public Processor<Object> get() {
        return new JoinStreamWindowAggregateProcessor(name, windowInfo, joinType, joinAction);
    }


    @SuppressWarnings("unchecked")
    private class JoinStreamWindowAggregateProcessor extends AbstractWindowProcessor<Object> {
        private String name;
        private final WindowInfo windowInfo;
        private final JoinType joinType;
        private ValueJoinAction<V1, V2, OUT> joinAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore<K, V1> leftWindowStore;
        private WindowStore<K, V2> rightWindowStore;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);


        public JoinStreamWindowAggregateProcessor(String name, WindowInfo windowInfo, JoinType joinType, ValueJoinAction<V1, V2, OUT> joinAction) {
            this.name = Utils.buildKey(name, JoinStreamWindowAggregateProcessor.class.getSimpleName());
            this.windowInfo = windowInfo;
            this.joinType = joinType;
            this.joinAction = joinAction;
        }

        @Override
        public void preProcess(StreamContext<Object> context) throws Throwable {
            super.preProcess(context);
            leftWindowStore = new WindowStore<>(super.waitStateReplay(), WindowState::byte2WindowState, WindowState::windowState2Byte);
            rightWindowStore = new WindowStore<>(super.waitStateReplay(), WindowState::byte2WindowState, WindowState::windowState2Byte);
            String stateTopicName = getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, getSourceBrokerName(), getSourceQueueId());
        }


        @Override
        public void process(Object data) throws Throwable {
            Throwable throwable = errorReference.get();
            if (throwable != null) {
                errorReference.set(null);
                throw throwable;
            }

            Object key = this.context.getKey();
            long time = this.context.getDataTime();
            Properties header = this.context.getHeader();
            long watermark = this.context.getWatermark();
            WindowInfo.JoinStream stream = (WindowInfo.JoinStream) header.get("addTagToStream");

            if (time < watermark) {
                //已经触发，丢弃数据
                return;
            }

            StreamType streamType = stream.getStreamType();


            store(key, data, time, streamType);

            fire(watermark, streamType);
        }


        private void store(Object key, Object data, long time, StreamType streamType) throws Throwable {
            String name = Utils.buildKey(this.name, streamType.name());
            List<Window> windows = super.calculateWindow(windowInfo, time);
            for (Window window : windows) {
                logger.debug("timestamp=" + time + ". time -> window: " + Utils.format(time) + "->" + window);

                WindowKey windowKey = new WindowKey(name, super.toHexString(key), window.getEndTime(), window.getStartTime());

                switch (streamType) {
                    case LEFT_STREAM:
                        WindowState<K, V1> leftState = new WindowState<>((K) key, (V1) data, time);
                        this.leftWindowStore.put(stateTopicMessageQueue, windowKey, leftState);
                    case RIGHT_STREAM:
                        WindowState<K, V2> rightState = new WindowState<>((K) key, (V2) data, time);
                        this.rightWindowStore.put(stateTopicMessageQueue, windowKey, rightState);
                }
            }
        }

        private void fire(long watermark, StreamType streamType) throws Throwable {
            String leftWindow = Utils.buildKey(this.name, StreamType.LEFT_STREAM.name());
            WindowKey leftWindowKey = new WindowKey(leftWindow, null, watermark, 0L);
            List<Pair<WindowKey, WindowState<K, V1>>> leftPairs = this.leftWindowStore.searchLessThanWatermark(leftWindowKey);

            String rightWindow = Utils.buildKey(this.name, StreamType.RIGHT_STREAM.name());
            WindowKey rightWindowKey = new WindowKey(rightWindow, null, watermark, 0L);
            List<Pair<WindowKey, WindowState<K, V2>>> rightPairs = this.rightWindowStore.searchLessThanWatermark(rightWindowKey);


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

                                OUT out = this.joinAction.apply(o1, o2);
                                this.context.forward(out);
                            }
                        }
                    }
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

                                OUT out = this.joinAction.apply(o1, o2);
                                this.context.forward(out);
                            }
                        case RIGHT_STREAM:
                            //do nothing.
                    }
            }

            //删除状态
            for (Pair<WindowKey, WindowState<K, V1>> leftPair : leftPairs) {
                for (Pair<WindowKey, WindowState<K, V2>> rightPair : rightPairs) {
                    this.leftWindowStore.deleteByKey(leftPair.getKey());
                    this.rightWindowStore.deleteByKey(rightPair.getKey());
                }
            }
        }

    }
}
