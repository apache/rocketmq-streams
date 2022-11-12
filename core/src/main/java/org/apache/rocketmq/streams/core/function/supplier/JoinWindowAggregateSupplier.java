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
import java.util.function.Supplier;

public class JoinWindowAggregateSupplier<K, V1, V2, OUT> implements Supplier<Processor<? super OUT>> {
    private static final Logger logger = LoggerFactory.getLogger(JoinWindowAggregateSupplier.class.getName());

    private WindowInfo windowInfo;
    private final ValueJoinAction<V1, V2, OUT> joinAction;
    private JoinType joinType;
    private TypeWrapper wrapper;

    public JoinWindowAggregateSupplier(WindowInfo windowInfo, ValueJoinAction<V1, V2, OUT> joinAction) {
        this.windowInfo = windowInfo;
        this.joinType = windowInfo.getJoinStream().getJoinType();
        this.joinAction = joinAction;
        this.wrapper = TypeExtractor.find(joinAction, "apply");
    }

    @Override
    public Processor<Object> get() {
        return new JoinStreamWindowAggregateProcessor(windowInfo, joinType, joinAction, wrapper);
    }


    private class JoinStreamWindowAggregateProcessor extends AbstractWindowProcessor<Object> {
        private final WindowInfo windowInfo;
        private final JoinType joinType;
        private ValueJoinAction<V1, V2, OUT> joinAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;
        private TypeWrapper wrapper;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);


        public JoinStreamWindowAggregateProcessor(WindowInfo windowInfo, JoinType joinType, ValueJoinAction<V1, V2, OUT> joinAction, TypeWrapper wrapper) {
            this.windowInfo = windowInfo;
            this.joinType = joinType;
            this.joinAction = joinAction;
            this.wrapper = wrapper;
        }

        @Override
        public void preProcess(StreamContext<Object> context) throws Throwable {
            super.preProcess(context);
            this.windowStore = new WindowStore(super.waitStateReplay());

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
            List<Window> windows = super.calculateWindow(windowInfo, time);
            for (Window window : windows) {
                logger.debug("timestamp=" + time + ". time -> window: " + Utils.format(time) + "->" + window);

                //todo key 怎么转化成对应的string，只和key的值有关系
                //相同key，value替换成最新
                String windowKey = Utils.buildKey(streamType.name(), String.valueOf(window.getEndTime()), String.valueOf(window.getStartTime()), key.toString());
                WindowState<Object, Object> state = new WindowState<>(key, data, time);

                this.windowStore.put(this.stateTopicMessageQueue, windowKey, state);

                logger.debug("put key into store, key: " + windowKey);
            }
        }

        private void fire(long watermark, StreamType streamType) throws Throwable {
            String leftKeyPrefix = Utils.buildKey(StreamType.LEFT_STREAM.name(), String.valueOf(watermark));
            String rightKeyPrefix = Utils.buildKey(StreamType.RIGHT_STREAM.name(), String.valueOf(watermark));

            TypeReference<WindowState<K, V1>> leftType = new TypeReference<WindowState<K, V1>>() {
                @Override
                public Type getType() {
                    Type[] types = new Type[2];
                    types[0] = JoinStreamWindowAggregateProcessor.this.context.getKey().getClass();
                    types[1] = wrapper.getParameterTypes()[0];
                    return ParameterizedTypeImpl.make(WindowState.class, types, null);
                }
            };
            List<Pair<String, WindowState<K, V1>>> leftPairs = this.windowStore.searchLessThanKeyPrefix(leftKeyPrefix, leftType);


            TypeReference<WindowState<K, V2>> rightType = new TypeReference<WindowState<K, V2>>() {
                @Override
                public Type getType() {
                    Type[] types = new Type[2];
                    types[0] = JoinStreamWindowAggregateProcessor.this.context.getKey().getClass();
                    types[1] = wrapper.getParameterTypes()[1];
                    return ParameterizedTypeImpl.make(WindowState.class, types, null);
                }
            };
            List<Pair<String, WindowState<K, V2>>> rightPairs = this.windowStore.searchLessThanKeyPrefix(rightKeyPrefix, rightType);


            if (leftPairs.size() == 0 && rightPairs.size() == 0) {
                return;
            }

            leftPairs.sort(Comparator.comparing(Pair::getObject1));
            rightPairs.sort(Comparator.comparing(Pair::getObject1));

            switch (joinType) {
                case INNER_JOIN:
                    //匹配上才触发
                    for (Pair<String, WindowState<K, V1>> leftPair : leftPairs) {
                        String leftPrefix = leftPair.getObject1().substring(0, leftPair.getObject1().length() - StreamType.LEFT_STREAM.name().length());

                        for (Pair<String, WindowState<K, V2>> rightPair : rightPairs) {
                            String rightPrefix = rightPair.getObject1().substring(0, rightPair.getObject1().length() - StreamType.RIGHT_STREAM.name().length());

                            //相同window中相同key，聚合
                            if (leftPrefix.equals(rightPrefix)) {
                                //do fire
                                V1 o1 = leftPair.getObject2().getValue();
                                V2 o2 = rightPair.getObject2().getValue();

                                OUT out = this.joinAction.apply(o1, o2);
                                this.context.forward(out);
                            }
                        }
                    }
                case LEFT_JOIN:
                    switch (streamType) {
                        case LEFT_STREAM:
                            //左流全部触发，不管匹配上没
                            for (Pair<String, WindowState<K, V1>> leftPair : leftPairs) {
                                String leftPrefix = leftPair.getObject1().substring(0, leftPair.getObject1().length() - StreamType.LEFT_STREAM.name().length());
                                Pair<String, WindowState<K, V2>> targetPair = null;
                                for (Pair<String, WindowState<K, V2>> rightPair : rightPairs) {
                                    if (rightPair.getObject1().startsWith(leftPrefix)) {
                                        targetPair = rightPair;
                                        break;
                                    }
                                }

                                //fire
                                V1 o1 = leftPair.getObject2().getValue();
                                V2 o2 = null;
                                if (targetPair != null) {
                                    o2 = targetPair.getObject2().getValue();
                                }

                                OUT out = this.joinAction.apply(o1, o2);
                                this.context.forward(out);
                            }
                        case RIGHT_STREAM:
                            //do nothing.
                    }
            }

            //删除状态
            for (Pair<String, WindowState<K, V1>> leftPair : leftPairs) {
                for (Pair<String, WindowState<K, V2>> rightPair : rightPairs) {
                    this.windowStore.deleteByKey(leftPair.getObject1());
                    this.windowStore.deleteByKey(rightPair.getObject1());
                }
            }
        }


//        @SuppressWarnings("unchecked")
//        private void fireWindowEndTimeLassThanWatermark(long watermark, K key) throws Throwable {
//            String keyPrefix = Utils.buildKey(String.valueOf(watermark), key.toString());
//
//            Class<?> temp = WindowState.class;
//            Class<WindowState<K, OV>> type = (Class<WindowState<K, OV>>) temp;
//
//            List<Pair<String, WindowState<K, OV>>> pairs = this.windowStore.searchLessThanKeyPrefix(keyPrefix, type);
//
//            //pairs中最后一个时间最小，应该最先触发
//            for (int i = pairs.size() - 1; i >= 0; i--) {
//
//                Pair<String, WindowState<K, OV>> pair = pairs.get(i);
//
//                WindowState<K, OV> value = pair.getObject2();
//
//                Data<K, OV> result = new Data<>(value.getKey(), value.getValue(), value.getTimestamp());
//                Data<K, V> convert = super.convert(result);
//
//                String windowKey = pair.getObject1();
//                if (logger.isDebugEnabled()) {
//                    String[] split = Utils.split(windowKey);
//                    long windowBegin = Long.parseLong(split[2]);
//                    long windowEnd = Long.parseLong(split[1]);
//                    logger.debug("fire window, windowKey={}, window: [{} - {}], data to next:[{}]", windowKey, Utils.format(windowBegin), Utils.format(windowEnd), convert);
//                }
//
//                this.context.forward(convert.getValue());
//
//                //删除状态
//                this.windowStore.deleteByKey(windowKey);
//            }
//        }
//

    }

    public interface JoinProcessor<V1, V2> {
        void process(V1 o1, V2 o2) throws Throwable;
    }
}
