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
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.running.StreamContext;
import org.apache.rocketmq.streams.core.util.ColumnFamilyUtil;
import org.apache.rocketmq.streams.core.window.JoinType;
import org.apache.rocketmq.streams.core.window.StreamType;
import org.apache.rocketmq.streams.core.state.StateStore;
import org.apache.rocketmq.streams.core.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.function.Supplier;

public class JoinAggregateSupplier<K, V1, V2, OUT> implements Supplier<Processor<? super OUT>> {
    private static final Logger logger = LoggerFactory.getLogger(JoinAggregateSupplier.class.getName());

    private String name;
    private JoinType joinType;
    private final ValueJoinAction<V1, V2, OUT> joinAction;

    public JoinAggregateSupplier(String name, JoinType joinType, ValueJoinAction<V1, V2, OUT> joinAction) {
        this.name = name;
        this.joinType = joinType;
        this.joinAction = joinAction;
    }

    @Override
    public Processor<Object> get() {
        return new JoinStreamAggregateProcessor(name, joinType, joinAction);
    }

    private class JoinStreamAggregateProcessor extends AbstractProcessor<Object> {
        private String name;
        private JoinType joinType;
        private final ValueJoinAction<V1, V2, OUT> joinAction;
        private MessageQueue stateTopicMessageQueue;
        private StateStore stateStore;


        public JoinStreamAggregateProcessor(String name, JoinType joinType, ValueJoinAction<V1, V2, OUT> joinAction) {
            this.name = name;
            this.joinType = joinType;
            this.joinAction = joinAction;
        }

        @Override
        public void preProcess(StreamContext<Object> context) throws RecoverStateStoreThrowable {
            super.preProcess(context);
            this.stateStore = super.waitStateReplay();
            String stateTopicName = context.getSourceTopic() + Constant.STATE_TOPIC_SUFFIX;
            this.stateTopicMessageQueue = new MessageQueue(stateTopicName, context.getSourceBrokerName(), context.getSourceQueueId());
        }

        @Override
        public void process(Object data) throws Throwable {
            Object key = this.context.getKey();
            Properties header = this.context.getHeader();
            StreamType streamType = (StreamType) header.get(Constant.STREAM_TAG);

            store(key, data, streamType);
            fire(key, data, streamType);
        }

        private void store(Object key, Object data, StreamType streamType) throws Throwable {
            String name = Utils.buildKey(this.name, streamType.name());

            switch (streamType) {
                case LEFT_STREAM:
                case RIGHT_STREAM: {
                    String storeKey = Utils.buildKey(name, super.toHexString(key));
                    byte[] keyBytes = Utils.object2Byte(storeKey);
                    byte[] valueBytes = super.object2Byte(data);

                    this.stateStore.put(stateTopicMessageQueue, ColumnFamilyUtil.VALUE_STATE_CF, keyBytes, valueBytes);
                    break;
                }
            }

        }

        @SuppressWarnings("unchecked")
        private void fire(Object key, Object data, StreamType streamType) throws Throwable {
            switch (streamType) {
                case LEFT_STREAM: {
                    String name = Utils.buildKey(this.name, StreamType.RIGHT_STREAM.name());
                    String storeKey = Utils.buildKey(name, super.toHexString(key));
                    byte[] keyBytes = Utils.object2Byte(storeKey);

                    byte[] bytes = this.stateStore.get(ColumnFamilyUtil.VALUE_STATE_CF, keyBytes);

                    if (joinType == JoinType.INNER_JOIN) {
                        if (bytes == null || bytes.length == 0) {
                            break;
                        }
                    } else if (joinType == JoinType.LEFT_JOIN) {
                        //no-op
                    } else {
                        throw new UnsupportedOperationException("unknown joinType = " + joinType);
                    }

                    V1 v1Data = (V1) data;
                    V2 v2Data = super.byte2Object(bytes);

                    doFire(v1Data, v2Data);
                    break;
                }
                case RIGHT_STREAM: {
                    if (joinType != JoinType.INNER_JOIN) {
                        break;
                    }

                    String name = Utils.buildKey(this.name, StreamType.LEFT_STREAM.name());
                    String storeKey = Utils.buildKey(name, super.toHexString(key));
                    byte[] keyBytes = Utils.object2Byte(storeKey);

                    byte[] bytes = this.stateStore.get(ColumnFamilyUtil.VALUE_STATE_CF, keyBytes);
                    if (bytes == null || bytes.length == 0) {
                        break;
                    }

                    V2 v2Data = (V2) data;
                    V1 v1Data = super.byte2Object(bytes);

                    doFire(v1Data, v2Data);
                    break;
                }
            }

            //todo 是否需要删除状态？
        }

        private void doFire(V1 v1Data, V2 v2Data) throws Throwable {
            OUT out = this.joinAction.apply(v1Data, v2Data);

            Data<K, OUT> result = new Data<>(this.context.getKey(), out, this.context.getDataTime(), this.context.getHeader());
            Data<K, Object> convert = super.convert(result);
            this.context.forward(convert);
        }
    }


}
