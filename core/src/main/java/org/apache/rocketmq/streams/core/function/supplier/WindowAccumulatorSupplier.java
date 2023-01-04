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
import org.apache.rocketmq.streams.core.exception.RecoverStateStoreThrowable;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractWindowProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.window.Window;
import org.apache.rocketmq.streams.core.window.WindowInfo;
import org.apache.rocketmq.streams.core.window.WindowKey;
import org.apache.rocketmq.streams.core.window.WindowState;
import org.apache.rocketmq.streams.core.window.WindowStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class WindowAccumulatorSupplier<K, V, R, OV> implements Supplier<Processor<V>> {
    private static final Logger logger = LoggerFactory.getLogger(WindowAccumulatorSupplier.class.getName());
    private final String name;
    private WindowInfo windowInfo;
    private SelectAction<R, V> selectAction;
    private Accumulator<R, OV> accumulator;

    public WindowAccumulatorSupplier(String name, WindowInfo windowInfo,
                                     SelectAction<R, V> selectAction, Accumulator<R, OV> accumulator) {
        this.name = name;
        this.windowInfo = windowInfo;
        this.selectAction = selectAction;
        this.accumulator = accumulator;
    }

    @Override
    public Processor<V> get() {
        WindowInfo.WindowType windowType = windowInfo.getWindowType();
        switch (windowType) {
            case SLIDING_WINDOW:
            case TUMBLING_WINDOW:
                return new WindowAggregateProcessor(name, windowInfo, selectAction, accumulator);
            case SESSION_WINDOW:
                return new SessionWindowAggregateProcessor(name, windowInfo, selectAction, accumulator);
            default:
                throw new RuntimeException("window type is error, WindowType=" + windowType);
        }
    }


    private class WindowAggregateProcessor extends CommonWindowFire {
        private final WindowInfo windowInfo;
        private String name;
        private MessageQueue stateTopicMessageQueue;
        private SelectAction<R, V> selectAction;
        private Accumulator<R, OV> accumulator;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

        public WindowAggregateProcessor(String name, WindowInfo windowInfo, SelectAction<R, V> selectAction, Accumulator<R, OV> accumulator) {
            this.name = name + WindowAggregateProcessor.class.getSimpleName();
            this.windowInfo = windowInfo;
            this.selectAction = selectAction;
            this.accumulator = accumulator;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws RecoverStateStoreThrowable {
            super.preProcess(context);
            this.windowStore = new WindowStore<>(super.waitStateReplay(), WindowState::byte2WindowState, WindowState::windowState2Byte);

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
                logger.warn("discard data:[{}], watermark[{}] > time[{}],", data, watermark, time);
                return;
            }

            //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
            fireWindowEndTimeLassThanWatermark(watermark, name, key);

            //f(time) -> List<Window>
            List<Window> windows = super.calculateWindow(windowInfo, time);
            for (Window window : windows) {
                logger.debug("timestamp=" + time + ". time -> window: " + Utils.format(time) + "->" + window);

                //f(Window + key, store) -> oldValue
                //todo key 怎么转化成对应的string，只和key的值有关系
                WindowKey windowKey = new WindowKey(name, super.toHexString(key), window.getEndTime(), window.getStartTime());
                WindowState<K, Accumulator<R, OV>> oldState = this.windowStore.get(windowKey);

                //f(oldValue, Agg) -> newValue
                Accumulator<R, OV> storeAccumulator;
                if (oldState == null || oldState.getValue() == null) {
                    storeAccumulator = accumulator.clone();
                } else {
                    storeAccumulator = oldState.getValue();
                }

                R select = selectAction.select(data);
                storeAccumulator.addValue(select);

                //f(Window + key, newValue, store)
                WindowState<K, Accumulator<R, OV>> state = new WindowState<>(key, storeAccumulator, time);
                this.windowStore.put(stateTopicMessageQueue, windowKey, state);
            }

            try {
                //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
                fireWindowEndTimeLassThanWatermark(watermark, name, key);
            } catch (Throwable t) {
                errorReference.compareAndSet(null, t);
            }
        }
    }

    private class SessionWindowAggregateProcessor extends CommonWindowFire {
        private final String name;
        private final WindowInfo windowInfo;
        private MessageQueue stateTopicMessageQueue;
        private SelectAction<R, V> selectAction;
        private Accumulator<R, OV> accumulator;

        public SessionWindowAggregateProcessor(String name, WindowInfo windowInfo, SelectAction<R, V> selectAction, Accumulator<R, OV> accumulator) {
            this.name = name + SessionWindowAggregateProcessor.class.getSimpleName();
            this.windowInfo = windowInfo;
            this.selectAction = selectAction;
            this.accumulator = accumulator;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws RecoverStateStoreThrowable {
            super.preProcess(context);
            super.windowStore = new WindowStore<>(super.waitStateReplay(), WindowState::byte2WindowState, WindowState::windowState2Byte);

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
                Accumulator<R, OV> temp = accumulator.clone();
                R select = selectAction.select(data);
                temp.addValue(select);

                WindowState<K, Accumulator<R, OV>> state = new WindowState<>(key, temp, time);
                if (time < state.getRecordEarliestTimestamp()) {
                    //更新最早时间戳，用于状态触发时候，作为session 窗口的begin时间戳
                    state.setRecordEarliestTimestamp(time);
                }

                WindowKey windowKey = new WindowKey(name, super.toHexString(key), newSessionWindowTime.getValue(), newSessionWindowTime.getKey());
                logger.info("new session window, with key={}, valueTime={}, sessionBegin=[{}], sessionEnd=[{}]", key, time,
                        Utils.format(newSessionWindowTime.getKey()), Utils.format(newSessionWindowTime.getValue()));
                this.windowStore.put(stateTopicMessageQueue, windowKey, state);
            }
        }


        //使用前缀查询找到session state, 触发已经session out的 watermark
        @SuppressWarnings("unchecked")
        private Pair<Long/*sessionBegin*/, Long/*sessionEnd*/> fireIfSessionOut(K key, V data, long dataTime, long watermark) throws Throwable {
            WindowKey windowKeyPrefix = new WindowKey(name, null, 0L, 0L);

            List<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> pairs = this.windowStore.searchMatchKeyPrefix(windowKeyPrefix);

            if (pairs.size() == 0) {
                return new Pair<>(dataTime, dataTime + windowInfo.getSessionTimeout().toMilliseconds());
            }

            logger.debug("exist session state num={}", pairs.size());

            //sessionEndTime小的先触发
            Iterator<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> iterator = pairs.iterator();
            int count = 0;
            long lastStateSessionEnd = 0;
            long maxFireSessionEnd = Long.MIN_VALUE;

            while (iterator.hasNext()) {
                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = iterator.next();
                logger.debug("exist session state{}=[{}]", count++, pair);

                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> state = pair.getValue();

                long sessionEnd = windowKey.getWindowEnd();
                if (count == pairs.size()) {
                    lastStateSessionEnd = sessionEnd;
                }

                //先触发一遍，触发后从集合中删除
                if (sessionEnd < watermark) {
                    //触发state
                    fire(key, windowKey, state);
                    iterator.remove();
                    maxFireSessionEnd = Long.max(sessionEnd, maxFireSessionEnd);
                }
            }

            if (dataTime < maxFireSessionEnd) {
                logger.warn("late data, discard. key=[{}], data=[{}], dataTime < maxFireSessionEnd: [{}] < [{}]", key, data, dataTime, maxFireSessionEnd);
                return null;
            }

            boolean createNewSessionWindow = false;
            WindowKey needToDelete = null;

            //再次遍历，找到数据属于某个窗口，如果窗口已经关闭，则只计算新的值，如果窗口没有关闭则计算新值、更新窗口边界、存储状态、删除老值
            for (int i = 0; i < pairs.size(); i++) {
                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = pairs.get(i);

                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> state = pair.getValue();

                Accumulator<R, OV> value = state.getValue();

                if (windowKey.getWindowEnd() < dataTime) {
                    createNewSessionWindow = true;
                } else if (windowKey.getWindowStart() <= dataTime) {
                    logger.debug("data belong to exist session window.dataTime=[{}], window:[{} - {}]", dataTime, Utils.format(windowKey.getWindowStart()), Utils.format(windowKey.getWindowEnd()));
                    R select = selectAction.select(data);
                    value.addValue(select);

                    //更新state
                    state.setValue(value);
                    state.setRecordLastTimestamp(dataTime);
                    if (dataTime < state.getRecordEarliestTimestamp()) {
                        //更新最早时间戳，用于状态触发时候，作为session 窗口的begin时间戳
                        state.setRecordEarliestTimestamp(dataTime);
                    }

                    //如果是最后一个窗口，更新窗口结束时间
                    if (i == pairs.size() - 1) {
                        long mayBeSessionEnd = dataTime + windowInfo.getSessionTimeout().toMilliseconds();
                        if (windowKey.getWindowEnd() < mayBeSessionEnd) {
                            logger.debug("update exist session window, before:[{} - {}], after:[{} - {}]", Utils.format(windowKey.getWindowStart()), Utils.format(windowKey.getWindowEnd()),
                                    Utils.format(windowKey.getWindowStart()), Utils.format(mayBeSessionEnd));
                            //删除老状态
                            needToDelete = windowKey;
                            //需要保存的新状态
                            windowKey = new WindowKey(windowKey.getOperatorName(), windowKey.getKey2String(), mayBeSessionEnd, windowKey.getWindowStart());
                        }
                    }
                } else {
                    logger.warn("discard data: key=[{}], data=[{}], dataTime=[{}], watermark=[{}]", key, data, dataTime, watermark);
                }

                this.windowStore.put(stateTopicMessageQueue, windowKey, state);
                this.windowStore.deleteByKey(needToDelete);
            }

            if (pairs.size() == 0 || createNewSessionWindow) {
                return new Pair<>(lastStateSessionEnd, dataTime + windowInfo.getSessionTimeout().toMilliseconds());
            }
            return null;
        }


        private void fire(K key, WindowKey windowKey, WindowState<K, Accumulator<R, OV>> state) throws Throwable {
            long windowEnd = windowKey.getWindowEnd();
            long windowBegin;
            if (state.getRecordEarliestTimestamp() == Long.MAX_VALUE) {
                windowBegin = windowKey.getWindowStart();
            } else {
                windowBegin = state.getRecordEarliestTimestamp();
            }

            logger.info("fire session,windowKey={}, search keyPrefix={}, window: [{} - {}]", windowKey, key.toString(), Utils.format(windowBegin), Utils.format(windowEnd));

            Properties header = this.context.getHeader();
            header.put(Constant.WINDOW_START_TIME, windowBegin);
            header.put(Constant.WINDOW_END_TIME, windowEnd);

            Accumulator<R, OV> value = state.getValue();
            OV data = value.result(header);

            Data<K, OV> result = new Data<>(state.getKey(), data, state.getRecordLastTimestamp(), header);
            Data<K, V> convert = super.convert(result);

            this.context.forward(convert);

            //删除状态
            this.windowStore.deleteByKey(windowKey);
        }
    }

    public abstract class CommonWindowFire extends AbstractWindowProcessor<V> {
        protected WindowStore<K, Accumulator<R, OV>> windowStore;

        /**
         * 触发窗口结束时间 <= watermark 的窗口
         *
         * @param watermark
         * @param key
         * @throws Throwable
         */
        protected void fireWindowEndTimeLassThanWatermark(long watermark, String operatorName, K key) throws Throwable {
            WindowKey windowKeyWatermark = new WindowKey(operatorName, toHexString(key), watermark, 0L);

            List<Pair<WindowKey, WindowState<K, Accumulator<R, OV>>>> pairs = this.windowStore.searchLessThanWatermark(windowKeyWatermark);

            //pairs中最后一个时间最小，应该最先触发
            for (int i = pairs.size() - 1; i >= 0; i--) {

                Pair<WindowKey, WindowState<K, Accumulator<R, OV>>> pair = pairs.get(i);
                WindowKey windowKey = pair.getKey();
                WindowState<K, Accumulator<R, OV>> value = pair.getValue();

                Properties header = this.context.getHeader();
                header.put(Constant.WINDOW_START_TIME, windowKey.getWindowStart());
                header.put(Constant.WINDOW_END_TIME, windowKey.getWindowEnd());

                Accumulator<R, OV> rovAccumulator = value.getValue();
                OV data = rovAccumulator.result(header);

                Data<K, OV> result = new Data<>(value.getKey(), data, value.getRecordLastTimestamp(), header);
                Data<K, V> convert = super.convert(result);

                if (logger.isDebugEnabled()) {
                    logger.debug("fire window, windowKey={}, search watermark={}, window: [{} - {}], data to next:[{}]", windowKey.toString(),
                            watermark, Utils.format(windowKey.getWindowStart()), Utils.format(windowKey.getWindowEnd()), convert);
                }

                this.context.forward(convert);

                //删除状态
                this.windowStore.deleteByKey(windowKey);
            }
        }
    }
}
