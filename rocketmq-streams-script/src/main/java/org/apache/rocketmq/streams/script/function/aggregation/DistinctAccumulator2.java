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
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.common.utils.StringUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksdbState;

/**
 * distinct operator based rocksdb state
 *
 * @author arthur.liang
 */
@Function
@UDAFFunction("distinct2")
public class DistinctAccumulator2 implements IAccumulator<DistinctAccumulator2.DistinctAccum2, DistinctAccumulator2.DistinctAccum2> {

    public static final String DISTINCT_STATE_PREFIX = "__distinct__";
    private static final Integer PARAMETER_SIZE = 3;
    private static RocksdbState state = new RocksdbState();

    @Override
    public DistinctAccum2 createAccumulator() {
        return new DistinctAccum2();
    }

    @Override
    public DistinctAccum2 getValue(DistinctAccum2 accumulator) {
        return accumulator;
    }

    @Override
    public void accumulate(DistinctAccum2 accumulator, Object... parameters) {
        if (CollectionUtil.isEmpty(parameters) || parameters.length != PARAMETER_SIZE) {
            return;
        }
        try {
            String value = (String) parameters[0];
            String valueMd5 = StringUtil.createMD5Str(value);
            String windowInstanceId = (String) parameters[1];
            if (accumulator.windowInstanceId == null && windowInstanceId != null) {
                accumulator.windowInstanceId = windowInstanceId;
            }
            assert accumulator.windowInstanceId.equalsIgnoreCase(windowInstanceId);
            String groupByValue = (String) parameters[2];
            String groupByMd5 = StringUtil.createMD5Str(groupByValue);
            if (accumulator.groupByMd5 == null && groupByMd5 != null) {
                accumulator.groupByMd5 = groupByMd5;
            }
            assert accumulator.groupByMd5.equalsIgnoreCase(groupByMd5);
            String storeKey = MapKeyUtil.createKey(DISTINCT_STATE_PREFIX, accumulator.windowInstanceId, accumulator.groupByMd5, valueMd5);
            state.putIfAbsent(storeKey, value);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void merge(DistinctAccum2 accumulator, Iterable<DistinctAccum2> its) {
        Iterator<DistinctAccum2> it = its.iterator();
        while (it.hasNext()) {
            DistinctAccum2 commonAccumulator = it.next();
            if (commonAccumulator != null) {
                if (accumulator.windowInstanceId == null || accumulator.groupByMd5 == null) {
                    accumulator.windowInstanceId = commonAccumulator.windowInstanceId;
                    accumulator.groupByMd5 = commonAccumulator.groupByMd5;
                }
                assert accumulator.windowInstanceId.equalsIgnoreCase(commonAccumulator.windowInstanceId);
                assert accumulator.groupByMd5.equalsIgnoreCase(commonAccumulator.groupByMd5);
            }
        }
    }

    @Override
    public void retract(DistinctAccum2 accumulator, String... parameters) {
    }

    public static class DistinctAccum2 {
        public String windowInstanceId;
        public String groupByMd5;
    }

}