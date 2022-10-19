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
import org.apache.rocketmq.streams.core.metadata.Context;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;

import java.util.function.Supplier;

public class ValueChangeSupplier<T, O> implements Supplier<Processor<T>> {
    private final ValueMapperAction<T, O> valueMapperAction;


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

            if (convert instanceof Iterable) {
                Iterable<? extends O> iterable = (Iterable<? extends O>) convert;
                for (O item : iterable) {
                    Context<Object, O> before = new Context<>(this.context.getKey(), item);
                    Context<Object, T> result = convert(before);
                    this.context.forward(result);
                }
            } else {
                Context<Object, O> before = new Context<>(this.context.getKey(), convert);
                Context<Object, T> result = convert(before);
                this.context.forward(result);
            }
        }
    }

}
