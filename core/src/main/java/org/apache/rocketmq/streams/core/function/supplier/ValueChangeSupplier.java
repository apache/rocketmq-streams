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

public class ValueChangeSupplier<T, O> implements Supplier<Processor<T>> {
    private final ValueMapperAction<T, O> valueMapperAction;
    private static final Logger logger = LoggerFactory.getLogger(ValueChangeSupplier.class.getName());


    public ValueChangeSupplier(ValueMapperAction<T, O> valueMapperAction) {
        this.valueMapperAction = valueMapperAction;
    }

    @Override
    public Processor<T> get() {
        return new ValueMapperProcessor<>(this.valueMapperAction);
    }


    static class ValueMapperProcessor<T, O> extends AbstractProcessor<T> {
        private final ValueMapperAction<T, O> valueMapperAction;


        public ValueMapperProcessor(ValueMapperAction<T, O> valueMapperAction) {
            this.valueMapperAction = valueMapperAction;
        }

        @Override
        public void process(T data) throws Throwable {
            O convert = valueMapperAction.convert(data);
            if (convert == null) {
                logger.warn("[{}] converts to null, processor returns directly", data);
                return;
            }
            Data<Object, O> before = new Data<>(this.context.getKey(), convert, this.context.getDataTime(), this.context.getHeader());
            Data<Object, T> result = convert(before);
            this.context.forward(result);
        }
    }

}
