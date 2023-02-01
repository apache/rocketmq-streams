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
import org.apache.rocketmq.streams.core.function.accumulator.Accumulator;
import org.apache.rocketmq.streams.core.function.SelectAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.state.StateStore;

import java.util.function.Supplier;

public class AccumulatorSupplier<K, V, R, OV> implements Supplier<Processor<V>> {
    private final String currentName;
    private final String parentName;
    private SelectAction<R, V> selectAction;
    private Accumulator<R, OV> accumulator;

    public AccumulatorSupplier(String currentName, String parentName, SelectAction<R, V> selectAction, Accumulator<R, OV> accumulator) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.selectAction = selectAction;
        this.accumulator = accumulator;
    }

    @Override
    public Processor<V> get() {
        return new AccumulatorProcessor(currentName, parentName, selectAction, accumulator);
    }

    private class AccumulatorProcessor extends AbstractProcessor<V> {
        private final String currentName;
        private final String parentName;
        private StateStore stateStore;
        private MessageQueue stateTopicMessageQueue;
        private SelectAction<R, V> selectAction;
        private Accumulator<R, OV> accumulator;

        public AccumulatorProcessor(String currentName, String parentName, SelectAction<R, V> selectAction, Accumulator<R, OV> accumulator) {
            this.currentName = currentName;
            this.parentName = parentName;
            this.selectAction = selectAction;
            this.accumulator = accumulator;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws RecoverStateStoreThrowable {
            super.preProcess(context);
            this.stateStore = super.waitStateReplay();

            String stateTopicName = context.getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, context.getSourceBrokerName(), context.getSourceQueueId());
        }

        @Override
        public void process(V data) throws Throwable {
            K key = this.context.getKey();
            Accumulator<R, OV> value;

            byte[] keyBytes = super.object2Byte(key);

            byte[] valueBytes = stateStore.get(keyBytes);
            if (valueBytes == null || valueBytes.length == 0) {
                value = accumulator.clone();
            } else {
                value = super.byte2Object(valueBytes);
            }

            R select = selectAction.select(data);
            value.addValue(select);

            OV result = value.result(null);
            byte[] newValueBytes = super.object2Byte(value);

            stateStore.put(this.stateTopicMessageQueue, keyBytes, newValueBytes);

            Data<K, OV> temp = new Data<>(key, result, this.context.getDataTime(), this.context.getHeader());
            Data<K, V> convert = super.convert(temp);

            this.context.forward(convert);
        }

    }
}
