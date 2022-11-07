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


import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractWindowProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.runtime.operators.SessionWindowState;
import org.apache.rocketmq.streams.core.runtime.operators.Window;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.runtime.operators.WindowStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class WindowAggregateSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private static final Logger logger = LoggerFactory.getLogger(WindowAggregateSupplier.class.getName());
    private final String currentName;
    private final String parentName;
    private WindowInfo windowInfo;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public WindowAggregateSupplier(String currentName, String parentName, WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.windowInfo = windowInfo;
        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<V> get() {
        WindowInfo.WindowType windowType = windowInfo.getWindowType();
        switch (windowType) {
            case SLIDING_WINDOW:
            case TUMBLING_WINDOW:
                return new WindowAggregateProcessor(currentName, parentName, windowInfo, initAction, aggregateAction);
            case SESSION_WINDOW:
                return new SessionWindowAggregateProcessor(windowInfo, initAction, aggregateAction);
            default:
                throw new RuntimeException("window type is error, WindowType=" + windowType);
        }
    }


    private class WindowAggregateProcessor extends AbstractWindowProcessor<K, V> {
        private final String currentName;
        private final String parentName;
        private final WindowInfo windowInfo;

        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

        public WindowAggregateProcessor(String currentName, String parentName, WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
            this.currentName = currentName;
            this.parentName = parentName;
            this.windowInfo = windowInfo;
            this.initAction = initAction;
            this.aggregateAction = aggregateAction;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws Throwable {
            super.preProcess(context);
            this.windowStore = new WindowStore(super.waitStateReplay());

            String stateTopicName = getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, getSourceBrokerName(), getSourceQueueId());
        }

        /**
         * 维持一个watermark，小于watermark的数据都已经达到，触发窗口计算
         */
        @Override
        public void process(V data) throws Throwable {
            Throwable throwable = errorReference.get();
            if (throwable != null) {
                errorReference.set(null);
                throw throwable;
            }

            K key = this.context.getKey();
            long time = this.context.getDataTime();

            long watermark = this.context.getWatermark();
            if (time < watermark) {
                //已经触发，丢弃数据
                return;
            }

            //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
            fireWindowEndTimeLassThanWatermark(watermark, key);

            //f(time) -> List<Window>
            List<Window> windows = super.calculateWindow(windowInfo, time);
            for (Window window : windows) {
                logger.debug("timestamp=" + time + ". time -> window: " + Utils.format(time) + "->" + window);

                //f(Window + key, store) -> oldValue
                //todo key 怎么转化成对应的string，只和key的值有关系
                String windowKey = Utils.buildKey(key.toString(), String.valueOf(window.getEndTime()), String.valueOf(window.getStartTime()));
                WindowState<K, OV> oldState = this.windowStore.get(windowKey);

                //f(oldValue, Agg) -> newValue
                OV oldValue;
                if (oldState == null || oldState.getValue() == null) {
                    oldValue = initAction.get();
                } else {
                    oldValue = oldState.getValue();
                }

                OV newValue = this.aggregateAction.calculate(key, data, oldValue);
                //f(Window + key, newValue, store)
                WindowState<K, OV> state = new WindowState<>(key, newValue, time);
                this.windowStore.put(this.stateTopicMessageQueue, windowKey, state);

                logger.debug("put key into store, key: " + windowKey);
            }

            try {
                //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
                fireWindowEndTimeLassThanWatermark(watermark, key);
            } catch (Throwable t) {
                errorReference.compareAndSet(null, t);
            }
        }

        /**
         * 触发窗口结束时间 <= watermark 的窗口
         *
         * @param watermark
         * @param key
         * @throws Throwable
         */
        @SuppressWarnings("unchecked")
        private void fireWindowEndTimeLassThanWatermark(long watermark, K key) throws Throwable {
            String keyPrefix = Utils.buildKey(key.toString(), String.valueOf(watermark));

            Class<?> temp = WindowState.class;
            Class<WindowState<K, OV>> type = (Class<WindowState<K, OV>>) temp;

            List<Pair<String, WindowState<K, OV>>> pairs = this.windowStore.searchLessThanKeyPrefix(keyPrefix, type);

            //pairs中最后一个时间最小，应该最先触发
            for (int i = pairs.size() - 1; i >= 0; i--) {

                Pair<String, WindowState<K, OV>> pair = pairs.get(i);

                WindowState<K, OV> value = pair.getObject2();

                Data<K, OV> result = new Data<>(value.getKey(), value.getValue(), value.getTimestamp());
                Data<K, V> convert = super.convert(result);

                this.context.forward(convert);

                //删除状态
                this.windowStore.deleteByKey(pair.getObject1());
            }
        }
    }


    private class SessionWindowAggregateProcessor extends AbstractWindowProcessor<K, V> {
        private final WindowInfo windowInfo;
        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;

        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

        public SessionWindowAggregateProcessor(WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
            this.windowInfo = windowInfo;
            this.initAction = initAction;
            this.aggregateAction = aggregateAction;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws Throwable {
            super.preProcess(context);
            this.windowStore = new WindowStore(super.waitStateReplay());

            String stateTopicName = getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, getSourceBrokerName(), getSourceQueueId());
        }

        @Override
        public void process(V data) throws Throwable {
            K key = this.context.getKey();
            long time = this.context.getDataTime();
            long watermark = this.context.getWatermark();

            //本地存储里面搜索下
            boolean createNewSessionWindow = fireIfSessionOut(key, data, time, watermark);

            if (createNewSessionWindow) {
                OV oldValue = this.initAction.get();
                OV newValue = this.aggregateAction.calculate(key, data, oldValue);

                SessionWindowState<K, OV> state = new SessionWindowState<>(key, newValue, time, time);

                long sessionEndTime = time + windowInfo.getSessionTimeout().toMilliseconds();
                String windowKey = Utils.buildKey(key.toString(), String.valueOf(sessionEndTime), String.valueOf(time));

                logger.info("new session window, with key={}, valueTime={}, sessionBegin=[{}], sessionEnd=[{}]", key, time, time, sessionEndTime);
                store(windowKey, state);
            }
        }


        //使用前缀查询找到session state, 触发已经session out的 watermark
        @SuppressWarnings("unchecked")
        private boolean fireIfSessionOut(K key, V data, long dataTime, long watermark) throws Throwable {
            Class<?> temp = SessionWindowState.class;
            Class<SessionWindowState<K, OV>> type = (Class<SessionWindowState<K, OV>>) temp;

            List<Pair<String, SessionWindowState<K, OV>>> pairs = this.windowStore.searchMatchKeyPrefix(key.toString(), type);

            if (pairs.size() == 0) {
                return true;
            }

            logger.debug("session state num={}", pairs.size());

            long lastSessionEndTime = Long.MAX_VALUE;
            boolean calculated = false;

            //sessionEndTime小的先触发
            for (int i = 0; i < pairs.size(); i++) {
                Pair<String, SessionWindowState<K, OV>> pair = pairs.get(i);
                logger.debug("fire session state=[{}]", pair);

                String windowKey = pair.getObject1();
                SessionWindowState<K, OV> state = pair.getObject2();

                String[] split = Utils.split(windowKey);
                long sessionEnd = Long.parseLong(split[1]);
                long sessionBegin = Long.parseLong(split[2]);

                if (i == 0) {
                    lastSessionEndTime = sessionEnd;
                }

                if (sessionBegin <= dataTime && dataTime < sessionEnd) {
                    logger.info("find session state with dataTime=[{}], result windowKey=[{}]", dataTime, windowKey);
                    calculated = true;

                    OV newValue = this.aggregateAction.calculate(key, data, state.getValue());
                    state.setValue(newValue);
                    state.setTimestamp(dataTime);

                    store(windowKey, state);
                }

                if (sessionEnd < watermark) {
                    //触发state
                    fire(windowKey, state);
                }
            }

            boolean createNewSessionWindow = dataTime > lastSessionEndTime;
            if (!createNewSessionWindow && !calculated) {
                logger.warn("discard data: key=[{}], data=[{}], dataTime=[{}], watermark=[{}]", key, data, dataTime, watermark);
            }
            return createNewSessionWindow;
        }


        //存储状态
        private void store(String windowKey, SessionWindowState<K, OV> state) throws Throwable {
            this.windowStore.put(this.stateTopicMessageQueue, windowKey, state);
            logger.debug("put key into store, key: " + windowKey);
        }

        private void fire(String windowKey, SessionWindowState<K, OV> state) throws Throwable {
            String[] split = Utils.split(windowKey);
            long windowEnd = Long.parseLong(split[1]);
            long windowBegin = state.getEarliestTimestamp();

            Data<K, OV> result = new Data<>(state.getKey(), state.getValue(), state.getTimestamp());
            Data<K, V> convert = super.convert(result);

            this.context.forward(convert);

            //删除状态
            this.windowStore.deleteByKey(windowKey);
        }
    }
}
