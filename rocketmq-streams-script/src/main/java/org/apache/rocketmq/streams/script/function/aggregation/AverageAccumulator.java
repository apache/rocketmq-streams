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
import org.apache.rocketmq.streams.script.function.aggregation.AverageAccumulator.AverageAccum;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Function
@UDAFFunction("avg")
public class AverageAccumulator implements IAccumulator<Number, AverageAccum> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AverageAccumulator.class);

    public static class AverageAccum {

        public Number sum;

        public Number value;

        public int count;

    }

    @Override
    public AverageAccum createAccumulator() {
        return new AverageAccum();
    }

    @Override
    public Number getValue(AverageAccum accumulator) {
        return accumulator.value;
    }

    @Override
    public void accumulate(AverageAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            Number parameter = null;
            if (parameters[0] instanceof String) {
                parameter = Double.valueOf((String) parameters[0]);
            } else {
                parameter = (Number) parameters[0];
            }
            if (accumulator.value == null) {
                accumulator.value = parameter;
                accumulator.count = 1;
                accumulator.sum = parameter;
                return;
            }
            accumulator.sum = accumulator.sum.doubleValue() + parameter.doubleValue();
            accumulator.count += 1;
            accumulator.value = NumberUtils.stripTrailingZeros(accumulator.sum.doubleValue() / accumulator.count);
        } catch (Exception e) {
            LOGGER.error("The value is [{}}]", parameters[0]);
            throw e;
        }
    }

    @Override
    public void merge(AverageAccum accumulator, Iterable<AverageAccum> its) {
        for (AverageAccum next : its) {
            if (next == null) {
                continue;
            }
            if (accumulator.value == null) {
                accumulator.sum = next.sum;
                accumulator.count = next.count;
                accumulator.value = next.value;
            } else {
                accumulator.count = accumulator.count + next.count;
                accumulator.sum = accumulator.sum.doubleValue() + next.sum.doubleValue();
                accumulator.value = NumberUtils.stripTrailingZeros(accumulator.sum.doubleValue() / accumulator.count);
            }
        }
    }

    @Override
    public void retract(AverageAccum accumulator, String... parameters) {
        //TODO
    }

}