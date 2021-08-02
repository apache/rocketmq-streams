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
import java.util.Set;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("count")
public class CountAccumulator implements IAccumulator<Integer, CountAccumulator.CountAccum> {

    public static class CountAccum {
        public int count = 0;
    }

    protected Integer count = 0;

    @Override
    public CountAccum createAccumulator() {
        return new CountAccum();
    }

    @Override
    public Integer getValue(CountAccum accumulator) {
        return accumulator.count;
    }

    @Override
    public void accumulate(CountAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters) || parameters[0] == null) {
            return;
        }
        if (parameters[0] instanceof Set) {
            //count(distinct(xx))
            //FIXME a trick! use CountDistinctAccumulator instead of the following code
            accumulator.count = ((Set)parameters[0]).size();
        } else {
            accumulator.count += 1;
        }
    }

    @Override
    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        Integer sum = accumulator.count;
        Iterator<CountAccum> it = its.iterator();
        while (it.hasNext()) {
            CountAccum next = it.next();
            if (next != null) {
                sum += next.count;
            }
        }
        accumulator.count = sum;
    }

    @Override
    public void retract(CountAccum accumulator, String... parameters) {
        //TODO
    }

}
