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
import org.apache.rocketmq.streams.core.runtime.operators.StreamType;
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

public class JoinWindowAggregateSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private static final Logger logger = LoggerFactory.getLogger(JoinWindowAggregateSupplier.class.getName());
    private final String currentName;
    private final String parentName;
    private WindowInfo windowInfo;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public JoinWindowAggregateSupplier(String currentName, String parentName, WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.windowInfo = windowInfo;
        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<V> get() {
        WindowInfo.JoinStream joinStream = this.windowInfo.getJoinStream();
        StreamType streamType = joinStream.getStreamType();
        switch (streamType) {
            case LEFT_STREAM:
                return new LeftStreamWindowAggregateProcessor(windowInfo, initAction, aggregateAction);
            case RIGHT_STREAM:
            default:
                throw new RuntimeException("error stream type.");
        }
    }


    private class LeftStreamWindowAggregateProcessor extends AbstractWindowProcessor<K, V> {
        private final WindowInfo windowInfo;

        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;

        private final AtomicReference<Throwable> errorReference = new AtomicReference<>(null);


        public LeftStreamWindowAggregateProcessor(WindowInfo windowInfo, Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
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

                this.context.forward(convert.getValue());

                //删除状态
                this.windowStore.deleteByKey(windowKey);
            }
        }
    }
}
