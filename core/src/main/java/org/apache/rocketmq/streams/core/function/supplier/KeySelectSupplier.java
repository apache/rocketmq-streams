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
import org.apache.rocketmq.streams.core.function.KeySelectAction;
import org.apache.rocketmq.streams.core.metadata.Data;
import org.apache.rocketmq.streams.core.running.AbstractProcessor;
import org.apache.rocketmq.streams.core.running.Processor;
import org.checkerframework.checker.units.qual.K;

import java.util.function.Supplier;

public class KeySelectSupplier<KEY, T> implements Supplier<Processor<T>> {
    private final KeySelectAction<KEY, T> keySelectAction;

    public KeySelectSupplier(KeySelectAction<KEY, T> keySelectAction) {
        this.keySelectAction = keySelectAction;
    }

    @Override
    public Processor<T> get() {
        return new MapperProcessor(keySelectAction);
    }

    private class MapperProcessor extends AbstractProcessor<T> {
        private final KeySelectAction<KEY, T> keySelectAction;


        public MapperProcessor(KeySelectAction<KEY, T> keySelectAction) {
            this.keySelectAction = keySelectAction;
        }



        @Override
        public void process(T data) throws Throwable {
            KEY newKey = keySelectAction.select(data);
            Data<KEY, T> temp = new Data<>(newKey, data, this.context.getDataTime());
            this.context.forward(temp);
        }
    }
}
