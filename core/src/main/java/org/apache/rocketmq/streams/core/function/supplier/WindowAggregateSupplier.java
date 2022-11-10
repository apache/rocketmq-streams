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

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class WindowAggregateSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private static final Logger logger = LoggerFactory.getLogger(WindowAggregateSupplier.class.getName());

    private WindowInfo windowInfo;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public WindowAggregateSupplier(WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
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
                return new WindowAggregateProcessor(windowInfo, initAction, aggregateAction);
            case SESSION_WINDOW:
                return new SessionWindowAggregateProcessor(windowInfo, initAction, aggregateAction);
            default:
                throw new RuntimeException("window type is error, WindowType=" + windowType);
        }
    }


    private class WindowAggregateProcessor extends AbstractWindowProcessor<K, V> {
        private final WindowInfo windowInfo;

        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

        public WindowAggregateProcessor(WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
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
                if (newValue != null && newValue.equals(oldValue)) {
                    continue;
                }

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

                String windowKey = pair.getObject1();
                if (logger.isDebugEnabled()) {
                    String[] split = Utils.split(windowKey);
                    long windowBegin = Long.parseLong(split[2]);
                    long windowEnd = Long.parseLong(split[1]);
                    logger.debug("fire window, windowKey={}, window: [{} - {}], data to next:[{}]", windowKey, Utils.format(windowBegin), Utils.format(windowEnd), convert);
                }

                this.context.forward(convert);

                //删除状态
                this.windowStore.deleteByKey(windowKey);
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
            Pair<Long, Long> newSessionWindowTime = fireIfSessionOut(key, data, time, watermark);

            if (newSessionWindowTime != null) {
                OV oldValue = this.initAction.get();
                OV newValue = this.aggregateAction.calculate(key, data, oldValue);

                SessionWindowState<K, OV> state = new SessionWindowState<>(key, newValue, time, time);

                Long sessionBegin = newSessionWindowTime.getObject1();
                Long sessionEnd = newSessionWindowTime.getObject2();
                String windowKey = Utils.buildKey(key.toString(), String.valueOf(sessionEnd), String.valueOf(sessionBegin));

                logger.info("new session window, with key={}, valueTime={}, sessionBegin=[{}], sessionEnd=[{}]", key, time, sessionBegin, sessionEnd);
                store(windowKey, state);
            }
        }


        //使用前缀查询找到session state, 触发已经session out的 watermark
        @SuppressWarnings("unchecked")
        private Pair<Long/*sessionBegin*/, Long/*sessionEnd*/> fireIfSessionOut(K key, V data, long dataTime, long watermark) throws Throwable {
            Class<?> temp = SessionWindowState.class;
            Class<SessionWindowState<K, OV>> type = (Class<SessionWindowState<K, OV>>) temp;

            List<Pair<String, SessionWindowState<K, OV>>> pairs = this.windowStore.searchMatchKeyPrefix(key.toString(), type);

            if (pairs.size() == 0) {
                return new Pair<>(dataTime, dataTime + windowInfo.getSessionTimeout().toMilliseconds());
            }

            logger.debug("exist session state num={}", pairs.size());

            //sessionEndTime小的先触发
            Iterator<Pair<String, SessionWindowState<K, OV>>> iterator = pairs.iterator();
            int count = 0;
            long lastStateSessionEnd = 0;
            long maxFireSessionEnd = Long.MIN_VALUE;

            while (iterator.hasNext()) {
                Pair<String, SessionWindowState<K, OV>> pair = iterator.next();
                logger.debug("exist session state{}=[{}]", count++, pair);

                String windowKey = pair.getObject1();
                SessionWindowState<K, OV> state = pair.getObject2();

                String[] split = Utils.split(windowKey);
                long sessionEnd = Long.parseLong(split[1]);
                if (count == pairs.size()) {
                    lastStateSessionEnd = sessionEnd;
                }

                //先触发一遍，触发后从集合中删除
                if (sessionEnd < watermark) {
                    //触发state
                    fire(windowKey, state);
                    iterator.remove();
                    maxFireSessionEnd = Long.max(sessionEnd, maxFireSessionEnd);
                }
            }

            if (dataTime < maxFireSessionEnd) {
                logger.warn("late data, discard. key=[{}], data=[{}], dataTime < maxFireSessionEnd: [{}] < [{}]", key, data, dataTime, maxFireSessionEnd);
                return null;
            }

            boolean createNewSessionWindow = false;
            String needToDelete = null;

            //再次遍历，找到数据属于某个窗口，如果窗口已经关闭，则只计算新的值，如果窗口没有关闭则计算新值、更新窗口边界、存储状态、删除老值
            for (int i = 0; i < pairs.size(); i++) {
                Pair<String, SessionWindowState<K, OV>> pair = pairs.get(i);

                String windowKey = pair.getObject1();
                SessionWindowState<K, OV> state = pair.getObject2();

                String[] split = Utils.split(windowKey);
                long sessionEnd = Long.parseLong(split[1]);
                long sessionBegin = Long.parseLong(split[2]);

                if (sessionEnd < dataTime) {
                    createNewSessionWindow = true;
                } else if (sessionBegin <= dataTime) {
                    logger.debug("data belong to exist session window.dataTime=[{}], window:[{} - {}]", dataTime, Utils.format(sessionBegin), Utils.format(sessionEnd));
                    OV newValue = this.aggregateAction.calculate(key, data, state.getValue());

                    //更新state
                    state.setValue(newValue);
                    state.setTimestamp(dataTime);
                    if (dataTime < state.getEarliestTimestamp()) {
                        //更新最早时间戳，用于状态触发时候，作为session 窗口的begin时间戳
                        state.setEarliestTimestamp(dataTime);
                    }

                    //如果是最后一个窗口，更新窗口结束时间
                    if (i == pairs.size() - 1) {
                        long mayBeSessionEnd = dataTime + windowInfo.getSessionTimeout().toMilliseconds();
                        if (sessionEnd < mayBeSessionEnd) {
                            logger.debug("update exist session window, before:[{} - {}], after:[{} - {}]", Utils.format(sessionBegin), Utils.format(sessionEnd),
                                    Utils.format(sessionBegin), Utils.format(mayBeSessionEnd));
                            //删除老状态
                            needToDelete = windowKey;
                            windowKey = Utils.buildKey(key.toString(), String.valueOf(mayBeSessionEnd), String.valueOf(sessionBegin));
                        }
                    }
                } else {
                    logger.warn("discard data: key=[{}], data=[{}], dataTime=[{}], watermark=[{}]", key, data, dataTime, watermark);
                }

                store(windowKey, state);
                this.windowStore.deleteByKey(needToDelete);
            }

            if (pairs.size() == 0 || createNewSessionWindow) {
                return new Pair<>(lastStateSessionEnd, dataTime + windowInfo.getSessionTimeout().toMilliseconds());
            }
            return null;
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

            logger.info("fire session,windowKey={}, window: [{} - {}]", windowKey, Utils.format(windowBegin), Utils.format(windowEnd));

            Data<K, OV> result = new Data<>(state.getKey(), state.getValue(), state.getTimestamp());
            Data<K, V> convert = super.convert(result);

            this.context.forward(convert);

            //删除状态
            this.windowStore.deleteByKey(windowKey);
        }
    }
}
