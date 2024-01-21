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
@UDAFFunction("min")
public class MinAccumulator implements IAccumulator<String, MinAccumulator.MinAccum> {

    private transient static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Override
    public MinAccum createAccumulator() {
        return new MinAccum();
    }

    @Override
    public String getValue(MinAccum accumulator) {
        return accumulator.min;
    }

    @Override
    public void accumulate(MinAccum accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters)) {
            return;
        }
        try {
            if (parameters[0] instanceof Number) {
                Number input = (Number) parameters[0];
                if (accumulator.min == null) {
                    accumulator.min = input.toString();
                    return;
                }
                Number min = Double.parseDouble(accumulator.min);
                accumulator.min = min.doubleValue() <= input.doubleValue() ? accumulator.min : input.toString();
            } else if (parameters[0] instanceof Date) {
                try {
                    Date input = (Date) parameters[0];
                    if (accumulator.min == null) {
                        accumulator.min = dateFormat.format(input);
                        return;
                    }
                    Date min = dateFormat.parse(accumulator.min);
                    accumulator.min = min.before(input) ? accumulator.min : dateFormat.format(input);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            } else if (parameters[0] instanceof String) {
                String input = (String) parameters[0];
                if ("".equals(input)) {
                    return;
                }
                if (accumulator.min == null) {
                    accumulator.min = input;
                    return;
                }
                String min = accumulator.min;
                try {
                    Date originDate = dateFormat.parse(min);
                    Date inputDate = dateFormat.parse(input);
                    accumulator.min = originDate.before(inputDate) ? accumulator.min : input;
                } catch (ParseException exception) {
                    accumulator.min = min.compareTo(input) <= 0 ? accumulator.min : input;
                }
            }
        } catch (Exception e) {
            throw e;
        }
    }

    @Override
    public void merge(MinAccum accumulator, Iterable<MinAccum> its) {
        Iterator<MinAccum> iterator = its.iterator();
        while (iterator.hasNext()) {
            MinAccum next = iterator.next();
            if (next != null) {
                accumulate(accumulator, next.min);
            }
        }
    }

    @Override
    public void retract(MinAccum accumulator, String... parameters) {
        //TODO
    }

    public static class MinAccum {
        public String min;
    }

}
