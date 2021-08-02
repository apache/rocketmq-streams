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

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("distinct")
public class DistinctAccumulator implements IAccumulator<Set, DistinctAccumulator.DistinctAccum> {

    public static class DistinctAccum {
        public Set values = Collections.synchronizedSet(new HashSet<>());
    }

    @Override
    public DistinctAccum createAccumulator() {
        return new DistinctAccum();
    }

    @Override
    public Set getValue(DistinctAccum accumulator) {
        return accumulator.values;
    }

    @Override
    public void accumulate(DistinctAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters) || parameters[0] == null) {
            return;
        }
        try {
            accumulator.values.add(parameters[0]);
        } catch (Exception e) {
            //TODO
        }
    }

    @Override
    public void merge(DistinctAccum accumulator, Iterable<DistinctAccum> its) {
        Iterator<DistinctAccum> it = its.iterator();
        while (it.hasNext()) {
            DistinctAccum commonAccumulator = it.next();
            if (commonAccumulator != null) {
                accumulator.values.addAll(commonAccumulator.values);
            }
        }
    }

    @Override
    public void retract(DistinctAccum accumulator, String... parameters) {
        //TODO
    }

}