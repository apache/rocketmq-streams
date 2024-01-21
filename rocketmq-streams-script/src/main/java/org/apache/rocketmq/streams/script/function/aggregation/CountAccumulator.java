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
import java.util.Map;
import java.util.Set;
import org.apache.rocketmq.streams.common.utils.CollectionUtil;
import org.apache.rocketmq.streams.common.utils.MapKeyUtil;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;
import org.apache.rocketmq.streams.state.kv.rocksdb.RocksdbState;

/**
 * @author arthur.liang
 */
@Function
@UDAFFunction("count")
public class CountAccumulator implements IAccumulator<Integer, CountAccumulator.CountAccum> {

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
        if (parameters[0] instanceof DistinctAccumulator2.DistinctAccum2) {
            DistinctAccumulator2.DistinctAccum2 distinctAccum2 = (DistinctAccumulator2.DistinctAccum2) parameters[0];
            String prefix = MapKeyUtil.createKey(DistinctAccumulator2.DISTINCT_STATE_PREFIX, distinctAccum2.windowInstanceId, distinctAccum2.groupByMd5);
            RocksdbState state = new RocksdbState();
            Iterator<Map.Entry<String, String>> iterator = state.entryIterator(prefix);
            int sum = 0;
            while (iterator.hasNext()) {
                Map.Entry<String, String> entry = iterator.next();
                if (entry == null) {
                    break;
                }
                sum += 1;
            }
            accumulator.count = sum;
        } else if (parameters[0] instanceof Set) {
            accumulator.count = ((Set) parameters[0]).size();
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

    public static class CountAccum {
        public int count = 0;
    }

}
