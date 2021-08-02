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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("concat_agg")
public class ConcatAccumulator implements IAccumulator<String, ConcatAccumulator.ConcatAccum> {

    private static final int USE_DEFAULT_SEPARATOR = 1;

    private static final int USE_DEFINED_SEPARATOR = 2;

    public static class ConcatAccum {

        public String separator = ",";

        public List<String> values = new ArrayList<>();
    }

    @Override
    public ConcatAccum createAccumulator() {
        return new ConcatAccum();
    }

    @Override
    public String getValue(ConcatAccum accumulator) {
        StringBuffer buffer = new StringBuffer();
        for (int index = 0; index < accumulator.values.size(); index++) {
            buffer.append(accumulator.values.get(index));
            if (index != accumulator.values.size() - 1) {
                buffer.append(accumulator.separator);
            }
        }
        return buffer.toString();
    }

    @Override
    public void accumulate(ConcatAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters) || parameters[0] == null) {
            return;
        }
        try {
            if (USE_DEFAULT_SEPARATOR == parameters.length) {
                accumulator.values.add((String)parameters[0]);
            } else if (USE_DEFINED_SEPARATOR == parameters.length) {
                accumulator.separator = (String)parameters[0];
                accumulator.values.add((String)parameters[1]);
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void merge(ConcatAccum accumulator, Iterable<ConcatAccum> its) {
        if (accumulator == null) {
            accumulator = new ConcatAccum();
        }
        Iterator<ConcatAccum> iterator = its.iterator();
        while (iterator.hasNext()) {
            ConcatAccum next = iterator.next();
            if (next != null) {
                accumulator.values.addAll(next.values);
            }
        }
    }

    @Override
    public void retract(ConcatAccum accumulator, String... parameters) {
        //TODO
    }

}
