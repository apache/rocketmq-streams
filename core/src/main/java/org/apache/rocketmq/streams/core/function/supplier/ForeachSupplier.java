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

import org.apache.rocketmq.streams.core.function.ForeachAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;

import java.util.function.Supplier;

public class ForeachSupplier<T> implements Supplier<Processor<T>> {
    private ForeachAction<T> foreachAction;

    public ForeachSupplier(ForeachAction<T> foreachAction) {
        this.foreachAction = foreachAction;
    }

    @Override
    public Processor<T> get() {
        return new ForeachProcessor(this.foreachAction);
    }

    class ForeachProcessor extends AbstractProcessor<T> {
        private ForeachAction<T> foreachAction;

        public ForeachProcessor(ForeachAction<T> foreachAction) {
            this.foreachAction = foreachAction;
        }

        @Override
        public void process(T data) throws Throwable {
            this.foreachAction.apply(data);
            Data<Object, T> result = new Data<>(this.context.getKey(), data, this.context.getDataTime(), this.context.getHeader());
            this.context.forward(result);
        }
    }


}
