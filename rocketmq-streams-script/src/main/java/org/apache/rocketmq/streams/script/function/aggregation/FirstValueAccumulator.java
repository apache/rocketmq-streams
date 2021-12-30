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
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("FIRST_VALUE")
public class FirstValueAccumulator<T> implements IAccumulator<T, FirstValueAccumulator.FirstValue> {

    public static class FirstValue<T> {
        public T value;
    }

    @Override
    public FirstValue createAccumulator() {
        return new FirstValue();
    }

    @Override
    public T getValue(FirstValue accumulator) {
        return (T) accumulator.value;
    }

    @Override
    public void accumulate(FirstValue accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters) || parameters[0] == null) {
            return;
        }
        if (accumulator.value != null) {
            return;
        }
        accumulator.value = parameters[0];
    }

    @Override
    public void merge(FirstValue accumulator, Iterable<FirstValue> its) {
        if (accumulator.value != null) {
            return;
        }
        Iterator<FirstValue> it = its.iterator();
        while (it.hasNext()) {
            FirstValue next = it.next();
            if (next != null && next.value != null) {
                accumulator.value = next.value;
                return;
            }
        }
    }

    @Override
    public void retract(FirstValue accumulator, String... parameters) {
        //TODO
    }

}
