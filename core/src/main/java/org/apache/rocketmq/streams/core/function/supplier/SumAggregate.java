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


import org.apache.rocketmq.streams.core.function.AggregateAction;
import org.apache.rocketmq.streams.core.function.SelectAction;

public class SumAggregate<K, V> implements AggregateAction<K, V, Number> {
    private final SelectAction<? extends Number, V> selectAction;

    public SumAggregate(SelectAction<? extends Number, V> selectAction) {
        this.selectAction = selectAction;
    }

    @Override
    public Number calculate(K key, V value, Number accumulator) {
        Number number = selectAction.select(value);
        if (accumulator == null) {
            accumulator = number;
            return accumulator;
        }

        if (number instanceof Integer) {
            return accumulator.intValue() + number.intValue();
        } else if (number instanceof Long) {
            return accumulator.longValue() + number.longValue();
        } else if (number instanceof Double) {
            return accumulator.doubleValue() + number.doubleValue();
        } else if (number instanceof Float) {
            return accumulator.floatValue() + number.floatValue();
        } else {
            throw new UnsupportedOperationException("unsupported number type:" + number.getClass().getName());
        }
    }
}
