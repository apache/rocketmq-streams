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
import org.apache.rocketmq.streams.core.state.StateStore;

import java.util.ArrayList;
import java.util.List;
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


    private class WindowAggregateProcessor extends AbstractWindowProcessor<K,V> {
        private final String currentName;
        private final String parentName;
        private final WindowInfo windowInfo;

        private StateStore stateStore;
        private Supplier<OV> initAction;
        private AggregateAction<K, V, OV> aggregateAction;
        private MessageQueue stateTopicMessageQueue;

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
            this.stateStore = super.waitStateReplay();

            MessageExt originData = context.getOriginData();
            String stateTopicName = originData.getTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, originData.getBrokerName(), originData.getQueueId());
        }

        @Override
        public void process(V data) throws Throwable {
            Data<K, V> originData = this.context.getData();
            K key = originData.getKey();

            //f(key, store) -> firedTime
            //todo key转化为字符串
            String firedTimeKey = Constant.FIRED_TIME_PREFIX + key.toString();
            Long firedTimeOfKey = this.stateStore.get(firedTimeKey);
            // 判断数据时间是否小于firedTime，如果小于，直接丢弃
            if (1L < firedTimeOfKey) {
                return;
            }

            //f(time) -> List<Window>
            List<Window> windows = super.calculateWindow(0L, key);

            ArrayList<Window> firedWindow = new ArrayList<>();


            //f(Window + key, store) -> oldValue

            //f(oldValue, Agg) -> newValue

            //f(Window + key, newValue, store)

            //f(timeWheel, firedTime)

            //firedTime 触发该实例所属window；

            //f(key, fireTime, store)
        }



    }


}
