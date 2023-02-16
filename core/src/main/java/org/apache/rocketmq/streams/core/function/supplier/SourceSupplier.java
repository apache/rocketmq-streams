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
import org.apache.rocketmq.streams.core.exception.DeserializeThrowable;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.window.TimeType;
import org.apache.rocketmq.streams.core.serialization.KeyValueDeserializer;
import org.apache.rocketmq.streams.core.util.Pair;

import java.util.function.Supplier;

public class SourceSupplier<K, V> implements Supplier<Processor<V>> {
    private String topicName;
    private KeyValueDeserializer<K, V> deserializer;

    public SourceSupplier(String topicName, KeyValueDeserializer<K, V> deserializer) {
        this.topicName = topicName;
        this.deserializer = deserializer;
    }

    @Override
    public Processor<V> get() {
        return new SourceProcessorImpl(deserializer);
    }

    public interface SourceProcessor<K, V> extends Processor<V> {
        Pair<K, V> deserialize(String keyClass, String valueClass, byte[] data) throws DeserializeThrowable;

        long getTimestamp(MessageExt originData, TimeType timeType);
    }

    private class SourceProcessorImpl extends AbstractProcessor<V> implements SourceProcessor<K, V> {
        private KeyValueDeserializer<K, V> deserializer;

        public SourceProcessorImpl(KeyValueDeserializer<K, V> deserializer) {
            this.deserializer = deserializer;
        }

        @Override
        public Pair<K, V> deserialize(String keyClass, String valueClass, byte[] data) throws DeserializeThrowable {
            try {
                this.deserializer.configure(keyClass, valueClass);
                return this.deserializer.deserialize(data);
            }catch (Throwable t) {
                throw new DeserializeThrowable(t);
            }
        }

        @Override
        public long getTimestamp(MessageExt originData, TimeType timeType) {
            if (timeType == null) {
                return System.currentTimeMillis();
            } else if (timeType == TimeType.EVENT_TIME) {
                return originData.getBornTimestamp();
            } else if (timeType == TimeType.PROCESS_TIME) {
                return System.currentTimeMillis();
            } else {
                throw new IllegalStateException("unknown time type: " + timeType.getClass().getName());
            }
        }


        @Override
        public void process(V data) throws Throwable {
            //no-op
        }
    }
}
