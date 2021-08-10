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
package org.apache.rocketmq.streams.script.function.aggregation;

import java.util.Iterator;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.NumberUtils;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("sum")
public class SumAccumulator implements IAccumulator<Number, SumAccumulator.SumAccum> {

    public static class SumAccum {
        public Number sum;
    }

    @Override
    public SumAccum createAccumulator() {
        SumAccum accum = new SumAccum();
        accum.sum = 0;
        return accum;
    }

    @Override
    public Number getValue(SumAccum accumulator) {
        return accumulator.sum;
    }

    @Override
    public void accumulate(SumAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            if (parameters[0] instanceof Number) {
                Number value = (Number)parameters[0];
                accumulator.sum = NumberUtils.stripTrailingZeros(accumulator.sum.doubleValue() + value.doubleValue());
            } else if (parameters[0] instanceof String) {
                Number value = Double.valueOf((String)parameters[0]);
                accumulator.sum = NumberUtils.stripTrailingZeros(accumulator.sum.doubleValue() + value.doubleValue());
            } else {
                throw new RuntimeException("type error!");
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void merge(SumAccum accumulator, Iterable<SumAccum> its) {
        Iterator<SumAccum> iterator = its.iterator();
        while (iterator.hasNext()) {
            SumAccum next = iterator.next();
            if (next != null) {
                accumulate(accumulator, next.sum);
            }
        }
    }

    @Override
    public void retract(SumAccum accumulator, String... parameters) {
        //TODO
    }
}
