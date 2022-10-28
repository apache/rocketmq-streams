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


import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.core.common.Constant;
import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractWindowProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.runtime.operators.Window;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.runtime.operators.WindowState;
import org.apache.rocketmq.streams.core.runtime.operators.WindowStore;
import org.apache.rocketmq.streams.core.util.Pair;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

public class WindowAggregateSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private final String currentName;
    private final String parentName;
    private WindowInfo windowInfo;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public WindowAggregateSupplier(String currentName, String parentName, WindowInfo windowInfo,
                                   Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.windowInfo = windowInfo;
        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<V> get() {
        return new WindowAggregateProcessor(currentName, parentName, windowInfo, initAction, aggregateAction);
    }


    private class WindowAggregateProcessor extends AbstractWindowProcessor<K, V> {
        private final String currentName;
        private final String parentName;
        private final WindowInfo windowInfo;

        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;
        private MessageQueue stateTopicMessageQueue;
        private WindowStore windowStore;

        private AtomicReference<Throwable> errorReference = new AtomicReference<>(null);

        public WindowAggregateProcessor(String currentName, String parentName, WindowInfo windowInfo,
                                        Supplier<OV> initAction, AggregateAction<K, V, OV> aggregateAction) {
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

            MessageExt originData = context.getOriginData();
            String stateTopicName = originData.getTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, originData.getBrokerName(), originData.getQueueId());
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

            Data<K, V> originData = this.context.getData();
            K key = originData.getKey();
            Long time = originData.getTimestamp();
            long watermark = originData.getWatermark();

            if (time < watermark) {
                //已经触发，丢弃数据
                return;
            }

            //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
            fireWindowEndTimeLassThanWatermark(watermark, key);

            //f(time) -> List<Window>
            List<Window> windows = super.calculateWindow(windowInfo, time);
            for (Window window : windows) {
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
                WindowState<K, OV> state = new WindowState<>(key, newValue, time, watermark);
                this.windowStore.put(this.stateTopicMessageQueue, windowKey, state);
            }

            try {
                //如果存在窗口，且窗口结束时间小于watermark，触发这个窗口
                fireWindowEndTimeLassThanWatermark(watermark, key);
            } catch (Throwable t) {
                errorReference.compareAndSet(null, t);
            }
        }

        private void fireWindowEndTimeLassThanWatermark(long watermark, K key) throws Throwable {
            String keyPrefix = Utils.buildKey(key.toString(), String.valueOf(watermark));

            List<Pair<String, WindowState<K, OV>>> pairs = this.windowStore.searchByKeyPrefix(keyPrefix);

            //需要倒序向后算子传递，pairs中最后一个结果的key，entTime最小，应该最先触发
            for (int i = pairs.size() - 1; i >= 0; i--) {

                Pair<String, WindowState<K, OV>> pair = pairs.get(i);

                WindowState<K, OV> value = pair.getObject2();

                Data<K, OV> result = new Data<>(value.getKey(), value.getValue(), value.getTimestamp(), value.getWatermark());
                Data<K, V> convert = super.convert(result);

                this.context.forward(convert);
                //删除状态
                this.windowStore.deleteByKey(pair.getObject1());
            }
        }

    }


}
