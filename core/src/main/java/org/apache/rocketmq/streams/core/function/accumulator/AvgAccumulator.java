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

public class AvgAccumulator<V> implements Accumulator<V, Double> {
    private Double avg;
    private Integer num;

    @Override
    public void addValue(V value) {
        if (value instanceof Number) {
            Number number = (Number) value;
            Double valueToDouble = number.doubleValue();

            if (avg == null) {
                avg = valueToDouble;
                num = 1;
            } else {
                avg = avg + (valueToDouble - avg) / (num + 1);
                num++;
            }
        } else {
            throw new IllegalArgumentException("Calculate avg, input is not a number. value=" + value);
        }
    }

    @Override
    public void merge(Accumulator<V, Double> other) {
        if (other instanceof AvgAccumulator) {
            AvgAccumulator<V> otherAvgAccumulator = (AvgAccumulator) other;
            Integer numOther = otherAvgAccumulator.getNum();
            Double avgOther = otherAvgAccumulator.getAvg();

            avg = avg + numOther / (num + numOther) * (avgOther - avg);
            num = num + numOther;
        } else {
            throw new IllegalArgumentException("Merge avg, input is not a AvgAccumulator.");
        }
    }

    @Override
    public Double result(Properties context) {
        return avg;
    }

    @Override
    public Accumulator<V, Double> clone() {
        return new AvgAccumulator<>();
    }

    public Double getAvg() {
        return avg;
    }

    public void setAvg(Double avg) {
        this.avg = avg;
    }

    public Integer getNum() {
        return num;
    }

    public void setNum(Integer num) {
        this.num = num;
    }
}
