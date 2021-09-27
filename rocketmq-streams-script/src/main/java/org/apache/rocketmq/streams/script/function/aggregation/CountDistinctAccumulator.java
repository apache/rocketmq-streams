package org.apache.rocketmq.streams.script.function.aggregation;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import java.util.Iterator;
import net.agkn.hll.HLL;
import org.apache.rocketmq.streams.script.annotation.Function;
import org.apache.rocketmq.streams.script.annotation.UDAFFunction;
import org.apache.rocketmq.streams.script.service.IAccumulator;

/**
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

    public static class CountDistinctAccum {
        //TODO make sure init parameters here
        private final HLL hll = new HLL(30, 8);
    }
}
