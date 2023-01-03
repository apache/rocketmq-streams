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
package org.apache.rocketmq.streams.core.function.accumulator;

import java.util.Properties;

public class MinAccumulator<V> implements Accumulator<V, Double> {
     private Number min;

    @Override
    public void addValue(V value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            if (min == null) {
                min = number;
            } else {
                min = Math.min(min.doubleValue(), number.doubleValue());
            }
        } else {
            throw new IllegalArgumentException("min but not a number. value=" + value);
        }

    }

    @Override
    public void merge(Accumulator<V, Double> other) {
        min = Math.min(min.doubleValue(), other.result(null));
    }

    @Override
    public Double result(Properties context) {
        return min.doubleValue();
    }

    @Override
    public Accumulator<V, Double> clone() {
        return new MinAccumulator<>();
    }

    public Number getMin() {
        return min;
    }

    public void setMin(Number min) {
        this.min = min;
    }
}
