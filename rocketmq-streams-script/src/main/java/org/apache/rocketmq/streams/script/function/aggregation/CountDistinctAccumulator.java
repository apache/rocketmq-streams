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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Iterator;
import net.agkn.hll.HLL;
import org.apache.rocketmq.streams.common.interfaces.ISerialize;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;

/**
 * count(distinct) implementation based on hyper-log-log algorithm
 *
 * @author arthur.liang
 */
@Function
@UDAFFunction("count_distinct")
public class CountDistinctAccumulator implements IAccumulator<Long, CountDistinctAccumulator.CountDistinctAccum> {

    private static final int SEED = 20210926;

    private static final HashFunction HASH_PRODUCER = Hashing.murmur3_128(SEED);

    @Override public CountDistinctAccum createAccumulator() {
        return new CountDistinctAccum();
    }

    @Override public Long getValue(CountDistinctAccum accumulator) {
        return accumulator.hll.cardinality();
    }

    @Override public void accumulate(CountDistinctAccum accumulator, Object... parameters) {
        if (parameters != null && parameters[0] != null) {
            Long hashKey = HASH_PRODUCER.newHasher().putUnencodedChars(parameters[0].toString()).hash().asLong();
            accumulator.hll.addRaw(hashKey);
        }
    }

    @Override public void merge(CountDistinctAccum accumulator, Iterable<CountDistinctAccum> its) {
        Iterator<CountDistinctAccum> dvIterator = its.iterator();
        while (dvIterator.hasNext()) {
            CountDistinctAccum dvAccum = dvIterator.next();
            if (dvAccum != null && dvAccum.hll != null) {
                accumulator.hll.union(dvAccum.hll);
            }
        }
    }

    @Override public void retract(CountDistinctAccum accumulator, String... parameters) {

    }

    public static class CountDistinctAccum implements ISerialize {
        private final HLL hll = new HLL(30, 8);
    }
}
