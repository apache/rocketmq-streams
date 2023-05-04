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

import org.apache.rocketmq.streams.core.function.ValueMapperAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class MultiValueChangeSupplier<T, VR> implements Supplier<Processor<T>> {
    private final ValueMapperAction<T, ? extends Iterable<? extends VR>> valueMapperAction;
    private static final Logger logger = LoggerFactory.getLogger(MultiValueChangeSupplier.class.getName());

    public MultiValueChangeSupplier(ValueMapperAction<T, ? extends Iterable<? extends VR>> valueMapperAction) {
        this.valueMapperAction = valueMapperAction;
    }

    @Override
    public Processor<T> get() {
        return new MultiValueMapperProcessor<>(this.valueMapperAction);
    }

    static class MultiValueMapperProcessor<T, VR> extends AbstractProcessor<T> {
        private final ValueMapperAction<T, ? extends Iterable<? extends VR>> valueMapperAction;

        public MultiValueMapperProcessor(ValueMapperAction<T, ? extends Iterable<? extends VR>> valueMapperAction) {
            this.valueMapperAction = valueMapperAction;
        }

        @Override
        public void process(T data) throws Throwable {
            Iterable<? extends VR> convert = valueMapperAction.convert(data);

            if (convert == null) {
                logger.warn("[{}] converts to null, processor returns directly", data);
                return;
            }

            for (VR item : convert) {
                if (item == null) {
                    continue;
                }
                Data<Object, VR> before = new Data<>(this.context.getKey(), item, this.context.getDataTime(), this.context.getHeader());
                Data<Object, T> result = convert(before);
                this.context.forward(result);
            }
        }
    }
}
