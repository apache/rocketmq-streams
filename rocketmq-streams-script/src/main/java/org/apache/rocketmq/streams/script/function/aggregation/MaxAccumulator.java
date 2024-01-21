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

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;

@Function
@UDAFFunction("max")
public class MaxAccumulator implements IAccumulator<String, MaxAccumulator.MaxAccum> {

    private transient static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public MaxAccum createAccumulator() {
        return new MaxAccum();
    }

    @Override
    public String getValue(MaxAccum accumulator) {
        return accumulator.max;
    }

    @Override
    public void accumulate(MaxAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            if (parameters[0] instanceof Number) {
                Number input = (Number) parameters[0];
                if (accumulator.max == null) {
                    accumulator.max = input.toString();
                    return;
                }
                Number max = Double.parseDouble(accumulator.max);
                accumulator.max = max.doubleValue() >= input.doubleValue() ? accumulator.max : input.toString();
            } else if (parameters[0] instanceof Date) {
                try {
                    Date input = (Date) parameters[0];
                    if (accumulator.max == null) {
                        accumulator.max = dateFormat.format(input);
                        return;
                    }
                    Date max = dateFormat.parse(accumulator.max);
                    accumulator.max = max.after(input) ? accumulator.max : dateFormat.format(input);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (parameters[0] instanceof String) {
                String input = (String) parameters[0];
                if (accumulator.max == null) {
                    accumulator.max = input;
                    return;
                }
                String min = accumulator.max;
                try {
                    Date originDate = dateFormat.parse(min);
                    Date inputDate = dateFormat.parse(input);
                    accumulator.max = originDate.after(inputDate) ? accumulator.max : input;
                } catch (ParseException exception) {
                    accumulator.max = min.compareTo(input) >= 0 ? accumulator.max : input;
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void merge(MaxAccum accumulator, Iterable<MaxAccum> its) {
        Iterator<MaxAccum> iterator = its.iterator();
        while (iterator.hasNext()) {
            MaxAccum next = iterator.next();
            if (next != null) {
                accumulate(accumulator, next.max);
            }
        }
    }

    @Override
    public void retract(MaxAccum accumulator, String... parameters) {
        //TODO
    }

    public static class MaxAccum {
        public String max;
    }

}
