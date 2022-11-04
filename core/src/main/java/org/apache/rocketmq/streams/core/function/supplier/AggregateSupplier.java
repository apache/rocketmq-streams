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
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;

import java.util.function.Supplier;

public class AggregateSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private final String currentName;
    private final String parentName;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public AggregateSupplier(String currentName, String parentName, Supplier<OV> initAction,
                             AggregateAction<K, V, OV> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<V> get() {
        return new AggregateProcessor(currentName, parentName, initAction, aggregateAction);
    }

    private class AggregateProcessor extends AbstractProcessor<V> {
        private final String currentName;
        private final String parentName;
        private final Supplier<OV> initAction;
        private final AggregateAction<K, V, OV> aggregateAction;
        private StateStore stateStore;
        private MessageQueue stateTopicMessageQueue;

        public AggregateProcessor(String currentName, String parentName, Supplier<OV> initAction,
                                  AggregateAction<K, V, OV> aggregateAction) {
            this.currentName = currentName;
            this.parentName = parentName;
            this.initAction = initAction;
            this.aggregateAction = aggregateAction;
        }

        @Override
        public void preProcess(StreamContext<V> context) throws Throwable {
            super.preProcess(context);
            this.stateStore = super.waitStateReplay();

            String stateTopicName = getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, getSourceBrokerName(), getSourceQueueId());
        }

        @Override
        public void process(V data) throws Throwable {
            K key = this.context.getKey();
            OV value = stateStore.get(key);
            if (value == null) {
                value = initAction.get();
            }

            OV result = aggregateAction.calculate(key, data, value);

            stateStore.put(this.stateTopicMessageQueue, key, result);

            Data<K, OV> temp = new Data<>(key, result, this.context.getDataTime());
            Data<K, V> convert = super.convert(temp);

            this.context.forward(convert);
        }

    }
}
